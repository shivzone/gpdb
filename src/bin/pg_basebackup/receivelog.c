/*-------------------------------------------------------------------------
 *
 * receivelog.c - receive transaction log files using the streaming
 *				  replication protocol.
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/receivelog.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <sys/stat.h>
#include <unistd.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

/* local includes */
#include "receivelog.h"
#include "streamutil.h"

#include "libpq-fe.h"
#include "access/xlog_internal.h"

#include <librdkafka/rdkafka.h>


/* fd and filename for currently open WAL file */
static FILE *lsnfile = NULL;
static char *lsnfile_name = NULL;
static bool reportFlushPosition = false;
static XLogRecPtr lastFlushPosition = InvalidXLogRecPtr;

/* kafka variables */
static rd_kafka_t *kafka_handle; /* Producer instance handle */
static rd_kafka_topic_t *kafka_topic; /* Topic object */

static bool still_sending = true;		/* feedback still needs to be sent? */

static PGresult *HandleCopyStream(PGconn *conn, StreamCtl *stream,
				 XLogRecPtr *stoppos);
static int	CopyStreamPoll(PGconn *conn, long timeout_ms);
static int	CopyStreamReceive(PGconn *conn, long timeout, char **buffer);
static bool ProcessKeepaliveMsg(PGconn *conn, char *copybuf, int len,
					XLogRecPtr blockpos, int64 *last_status);
static bool ProcessXLogDataMsg(PGconn *conn, StreamCtl *stream, char *copybuf, int len,
				   XLogRecPtr *blockpos);
static PGresult *HandleEndOfCopyStream(PGconn *conn, StreamCtl *stream, char *copybuf,
					  XLogRecPtr blockpos, XLogRecPtr *stoppos);
static bool CheckCopyStreamStop(PGconn *conn, StreamCtl *stream, XLogRecPtr blockpos,
					XLogRecPtr *stoppos);
static long CalculateCopyStreamSleeptime(int64 now, int standby_message_timeout,
							 int64 last_status);

static bool ReadEndOfStreamingResult(PGresult *res, XLogRecPtr *startpos,
						 uint32 *timeline);

static void dr_msg_cb (rd_kafka_t *rk,
					   const rd_kafka_message_t *rkmessage, void *opaque);

/*
 * Close the destination (if open) along with the resume lsn file
 * On failure, prints an error message to stderr
 * and returns false, otherwise returns true.
 */
static bool
close_destination(StreamCtl *stream, XLogRecPtr pos)
{
	if (lsnfile != NULL)
	{
		fclose(lsnfile);
		lsnfile = NULL;
	}

	if(!kafka_handle)
		return true;

	rd_kafka_topic_destroy(kafka_topic);
	rd_kafka_destroy(kafka_handle);

	lastFlushPosition = pos;

	fprintf(stderr, "Closed kafka topic: %s\n", stream->destination);

	return true;
}

/*
 * Open the destination (if not open) along with the resume lsn file
 * On failure, prints an error message to stderr
 * and returns false, otherwise returns true.
 */
static bool
open_destination(StreamCtl *stream)
{
	// Open the lsn file
	if (lsnfile == NULL)
	{
		lsnfile_name = psprintf("%s.lsn", stream->destination);
		lsnfile = fopen(lsnfile_name, "w");
	}

	/* Kafka test start. */
	rd_kafka_conf_t *conf; /* Temporary configuration object */
	char errstr[512]; /* librdkafka API error reporting buffer */
	const char *brokers;    /* Argument: broker list */
	const char *topic; /* Argument: topic to produce to */

	brokers = "localhost:9092";
	topic = stream->destination;

	conf = rd_kafka_conf_new();

	/* Set bootstrap broker(s) as a comma-separated list of
	 * host or host:port (default port 9092).
	 * librdkafka will use the bootstrap brokers to acquire the full
	 * set of brokers from the cluster. */
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
						  errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		fprintf(stderr, "%s\n", errstr);
		return false;
	}

	/* Set the delivery report callback.
	 * This callback will be called once per message to inform
	 * the application if delivery succeeded or failed.
	 * See dr_msg_cb() above. */
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

	/*
	 * Create producer instance.
	 *
	 * NOTE: rd_kafka_new() takes ownership of the conf object
	 *       and the application must not reference it again after
	 *       this call.
	 */
	kafka_handle = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!kafka_handle)
	{
		fprintf(stderr,
				"%% Failed to create new producer: %s\n", errstr);
		return false;
	}
	fprintf(stderr, "Created kafka handle\n");

	/* Create topic object that will be reused for each message
	 * produced.Message delivery failed
	 *
	 *
	 *
	 * Both the producer instance (rd_kafka_t) and topic objects (topic_t)
	 * are long-lived objects that should be reused as much as possible.
	 */
	kafka_topic = rd_kafka_topic_new(kafka_handle, topic, NULL);
	if (!kafka_topic)
	{
		fprintf(stderr, "%% Failed to create topic object: %s\n",
				rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_destroy(kafka_handle);
		return false;
	}

	fprintf(stderr, "Created kafka topic: %s\n", topic);
	return true;
}

static bool
write_destination(StreamCtl *stream, char *message, int len) {

	retry:
	if (rd_kafka_produce(
		/* Topic object */
		kafka_topic,
		/* Use builtin partitioner to select partition*/
		RD_KAFKA_PARTITION_UA,
		/* Make a copy of the payload. */
		RD_KAFKA_MSG_F_COPY,
		/* Message payload (value) and length */
		message, len,
		/* Optional key and its length */
		NULL, 0,
		/* Message opaque, provided in
		 * delivery report callback as
		 * msg_opaque. */
		NULL) == -1)
	{
		/**
		 * Failed to *enqueue* message for producing.
		 */
		fprintf(stderr,
				"%% Failed to produce to topic %s: %s\n",
				rd_kafka_topic_name(kafka_topic),
				rd_kafka_err2str(rd_kafka_last_error()));

		/* Poll to handle delivery reports */
		if (rd_kafka_last_error() ==
			RD_KAFKA_RESP_ERR__QUEUE_FULL)
		{
			/* If the internal queue is full, wait for
			 * messages to be delivered and then retry.
			 * The internal queue represents both
			 * messages to be sent and messages that have
			 * been sent or failed, awaiting their
			 * delivery report callback to be called.
			 *
			 * The internal queue is limited by the
			 * configuration property
			 * queue.buffering.max.messages */
			rd_kafka_poll(kafka_handle, 1000/*block for max 1000ms*/);
			goto retry;
		}
	}
	else
	{
		fprintf(stderr, "%% Enqueued message (%zd bytes) "
						"for topic %s\n",
				len, rd_kafka_topic_name(kafka_topic));
	}

	/* A producer application should continually serve
	 * the delivery report queue by calling rd_kafka_poll()
	 * at frequent intervals.
	 * Either put the poll call in your main loop, or in a
	 * dedicated thread, or call it after every
	 * rd_kafka_produce() call.
	 * Just make sure that rd_kafka_poll() is still called
	 * during periods where you are not producing any messages
	 * to make sure previously produced messages have their
	 * delivery report callback served (and any other callbacks
	 * you register). */
	rd_kafka_poll(kafka_handle, 0/*non-blocking*/);

	rd_kafka_flush(kafka_handle, 10*1000 /* wait for max 10 seconds */);

	return true;
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, XLogRecPtr blockpos, int64 now, bool replyRequested)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(blockpos, &replybuf[len]);		/* write */
	len += 8;
	if (reportFlushPosition)
		fe_sendint64(lastFlushPosition, &replybuf[len]);		/* flush */
	else
		fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);		/* flush */
	len += 8;
	fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0;		/* replyRequested */
	len += 1;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		fprintf(stderr, _("%s: could not send feedback packet: %s"),
				progname, PQerrorMessage(conn));
		return false;
	}

	return true;
}

/*
 * Check that the server version we're connected to is supported by
 * ReceiveXlogStream().
 *
 * If it's not, an error message is printed to stderr, and false is returned.
 */
bool
CheckServerVersionForStreaming(PGconn *conn)
{
	int			minServerMajor,
				maxServerMajor;
	int			serverMajor;

	/*
	 * The message format used in streaming replication changed in 9.3, so we
	 * cannot stream from older servers. And we don't support servers newer
	 * than the client; it might work, but we don't know, so err on the safe
	 * side.
	 */
	minServerMajor = 903;
	maxServerMajor = PG_VERSION_NUM / 100;
	serverMajor = PQserverVersion(conn) / 100;
	if (serverMajor < minServerMajor)
	{
		const char *serverver = PQparameterStatus(conn, "server_version");

		fprintf(stderr, _("%s: incompatible server version %s; client does not support streaming from server versions older than %s\n"),
				progname,
				serverver ? serverver : "'unknown'",
				"9.3");
		return false;
	}
	else if (serverMajor > maxServerMajor)
	{
		const char *serverver = PQparameterStatus(conn, "server_version");

		fprintf(stderr, _("%s: incompatible server version %s; client does not support streaming from server versions newer than %s\n"),
				progname,
				serverver ? serverver : "'unknown'",
				PG_VERSION);
		return false;
	}
	return true;
}

/*
 * Receive a log stream starting at the specified position.
 *
 * Individual parameters are passed through the StreamCtl structure.
 *
 * If sysidentifier is specified, validate that both the system
 * identifier and the timeline matches the specified ones
 * (by sending an extra IDENTIFY_SYSTEM command)
 *
 * All received segments will be written to the directory
 * specified by destination. This will also fetch any missing timeline history
 * files.
 *
 * The stream_stop callback will be called every time data
 * is received, and whenever a segment is completed. If it returns
 * true, the streaming will stop and the function
 * return. As long as it returns false, streaming will continue
 * indefinitely.
 *
 * standby_message_timeout controls how often we send a message
 * back to the master letting it know our progress, in milliseconds.
 * This message will only contain the write location, and never
 * flush or replay.
 *
 * If 'partial_suffix' is not NULL, files are initially created with the
 * given suffix, and the suffix is removed once the file is finished. That
 * allows you to tell the difference between partial and completed files,
 * so that you can continue later where you left.
 *
 * If 'synchronous' is true, the received WAL is flushed as soon as written,
 * otherwise only when the WAL file is closed.
 *
 * Note: The log position *must* be at a log segment start!
 */
bool
ReceiveXlogStream(PGconn *conn, StreamCtl *stream)
{
	char		query[128];
	char		slotcmd[128];
	PGresult   *res;
	XLogRecPtr	stoppos;

	/*
	 * The caller should've checked the server version already, but doesn't do
	 * any harm to check it here too.
	 */
	if (!CheckServerVersionForStreaming(conn))
		return false;

	if (replication_slot != NULL)
	{
		/*
		 * Report the flush position, so the primary can know what WAL we'll
		 * possibly re-request, and remove older WAL safely.
		 *
		 * We only report it when a slot has explicitly been used, because
		 * reporting the flush position makes one eligible as a synchronous
		 * replica. People shouldn't include generic names in
		 * synchronous_standby_names, but we've protected them against it so
		 * far, so let's continue to do so in the situations when possible. If
		 * they've got a slot, though, we need to report the flush position,
		 * so that the master can remove WAL.
		 */
		reportFlushPosition = true;
		sprintf(slotcmd, "SLOT \"%s\" ", replication_slot);
	}
	else
	{
		reportFlushPosition = false;
		slotcmd[0] = 0;
	}

	if (stream->sysidentifier != NULL)
	{
		/* Validate system identifier hasn't changed */
		res = PQexec(conn, "IDENTIFY_SYSTEM");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr,
					_("%s: could not send replication command \"%s\": %s"),
					progname, "IDENTIFY_SYSTEM", PQerrorMessage(conn));
			PQclear(res);
			return false;
		}
		if (PQntuples(res) != 1 || PQnfields(res) < 3)
		{
			fprintf(stderr,
					_("%s: could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields\n"),
					progname, PQntuples(res), PQnfields(res), 1, 3);
			PQclear(res);
			return false;
		}
		if (strcmp(stream->sysidentifier, PQgetvalue(res, 0, 0)) != 0)
		{
			fprintf(stderr,
					_("%s: system identifier does not match between base backup and streaming connection\n"),
					progname);
			PQclear(res);
			return false;
		}
		if (stream->timeline > atoi(PQgetvalue(res, 0, 1)))
		{
			fprintf(stderr,
				_("%s: starting timeline %u is not present in the server\n"),
					progname, stream->timeline);
			PQclear(res);
			return false;
		}
		PQclear(res);
	}

	/*
	 * initialize flush position to starting point, it's the caller's
	 * responsibility that that's sane.
	 */
	lastFlushPosition = stream->startpos;

	while (1)
	{
		/*
		 * Before we start streaming from the requested location, check if the
		 * callback tells us to stop here.
		 */
		if (stream->stream_stop(stream->startpos, stream->timeline, false))
			return true;

		/* Initiate the replication stream at specified location */
		snprintf(query, sizeof(query), "START_REPLICATION %s%X/%X TIMELINE %u",
				 slotcmd,
				 (uint32) (stream->startpos >> 32), (uint32) stream->startpos,
				 stream->timeline);
		res = PQexec(conn, query);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			fprintf(stderr, _("%s: could not send replication command \"%s\": %s"),
					progname, "START_REPLICATION", PQresultErrorMessage(res));
			PQclear(res);
			return false;
		}
		PQclear(res);

		/* Stream the WAL */
		res = HandleCopyStream(conn, stream, &stoppos);
		if (res == NULL)
			goto error;

		/*
		 * Streaming finished.
		 *
		 * There are two possible reasons for that: a controlled shutdown, or
		 * we reached the end of the current timeline. In case of
		 * end-of-timeline, the server sends a result set after Copy has
		 * finished, containing information about the next timeline. Read
		 * that, and restart streaming from the next timeline. In case of
		 * controlled shutdown, stop here.
		 */
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			/*
			 * End-of-timeline. Read the next timeline's ID and starting
			 * position. Usually, the starting position will match the end of
			 * the previous timeline, but there are corner cases like if the
			 * server had sent us half of a WAL record, when it was promoted.
			 * The new timeline will begin at the end of the last complete
			 * record in that case, overlapping the partial WAL record on the
			 * the old timeline.
			 */
			uint32		newtimeline;
			bool		parsed;

			parsed = ReadEndOfStreamingResult(res, &stream->startpos, &newtimeline);
			PQclear(res);
			if (!parsed)
				goto error;

			/* Sanity check the values the server gave us */
			if (newtimeline <= stream->timeline)
			{
				fprintf(stderr,
						_("%s: server reported unexpected next timeline %u, following timeline %u\n"),
						progname, newtimeline, stream->timeline);
				goto error;
			}
			if (stream->startpos > stoppos)
			{
				fprintf(stderr,
						_("%s: server stopped streaming timeline %u at %X/%X, but reported next timeline %u to begin at %X/%X\n"),
						progname,
				stream->timeline, (uint32) (stoppos >> 32), (uint32) stoppos,
						newtimeline, (uint32) (stream->startpos >> 32), (uint32) stream->startpos);
				goto error;
			}

			/* Read the final result, which should be CommandComplete. */
			res = PQgetResult(conn);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr,
				   _("%s: unexpected termination of replication stream: %s"),
						progname, PQresultErrorMessage(res));
				PQclear(res);
				goto error;
			}
			PQclear(res);

			/*
			 * Loop back to start streaming from the new timeline. Always
			 * start streaming at the beginning of a segment.
			 */
			stream->timeline = newtimeline;
			stream->startpos = stream->startpos - (stream->startpos % XLOG_SEG_SIZE);
			continue;
		}
		else if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			PQclear(res);

			/*
			 * End of replication (ie. controlled shut down of the server).
			 *
			 * Check if the callback thinks it's OK to stop here. If not,
			 * complain.
			 */
			if (stream->stream_stop(stoppos, stream->timeline, false))
				return true;
			else
			{
				fprintf(stderr, _("%s: replication stream was terminated before stop point\n"),
						progname);
				goto error;
			}
		}
		else
		{
			/* Server returned an error. */
			fprintf(stderr,
					_("%s: unexpected termination of replication stream: %s"),
					progname, PQresultErrorMessage(res));
			PQclear(res);
			goto error;
		}
	}

error:
	return false;
}

/*
 * Helper function to parse the result set returned by server after streaming
 * has finished. On failure, prints an error to stderr and returns false.
 */
static bool
ReadEndOfStreamingResult(PGresult *res, XLogRecPtr *startpos, uint32 *timeline)
{
	uint32		startpos_xlogid,
				startpos_xrecoff;

	/*----------
	 * The result set consists of one row and two columns, e.g:
	 *
	 *	next_tli | next_tli_startpos
	 * ----------+-------------------
	 *		   4 | 0/9949AE0
	 *
	 * next_tli is the timeline ID of the next timeline after the one that
	 * just finished streaming. next_tli_startpos is the XLOG position where
	 * the server switched to it.
	 *----------
	 */
	if (PQnfields(res) < 2 || PQntuples(res) != 1)
	{
		fprintf(stderr,
				_("%s: unexpected result set after end-of-timeline: got %d rows and %d fields, expected %d rows and %d fields\n"),
				progname, PQntuples(res), PQnfields(res), 1, 2);
		return false;
	}

	*timeline = atoi(PQgetvalue(res, 0, 0));
	if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &startpos_xlogid,
			   &startpos_xrecoff) != 2)
	{
		fprintf(stderr,
			_("%s: could not parse next timeline's starting point \"%s\"\n"),
				progname, PQgetvalue(res, 0, 1));
		return false;
	}
	*startpos = ((uint64) startpos_xlogid << 32) | startpos_xrecoff;

	return true;
}

/*
 * The main loop of ReceiveXlogStream. Handles the COPY stream after
 * initiating streaming with the START_STREAMING command.
 *
 * If the COPY ends (not necessarily successfully) due a message from the
 * server, returns a PGresult and sets *stoppos to the last byte written.
 * On any other sort of error, returns NULL.
 */
static PGresult *
HandleCopyStream(PGconn *conn, StreamCtl *stream,
				 XLogRecPtr *stoppos)
{
	char	   *copybuf = NULL;
	int64		last_status = -1;
	XLogRecPtr	blockpos = stream->startpos;

	still_sending = true;

	while (1)
	{
		int			r;
		int64		now;
		long		sleeptime;

		/*
		 * Check if we should continue streaming, or abort at this point.
		 */
		if (!CheckCopyStreamStop(conn, stream, blockpos, stoppos))
			goto error;

		now = feGetCurrentTimestamp();

		/*
		 * If synchronous option is true, issue sync command as soon as there
		 * are WAL data which has not been flushed yet.
		 */
		if (stream->synchronous && lastFlushPosition < blockpos && kafka_handle)
		{
			lastFlushPosition = blockpos;

			/*
			 * Send feedback so that the server sees the latest WAL locations
			 * immediately.
			 */
			if (!sendFeedback(conn, blockpos, now, false))
				goto error;
			last_status = now;
		}

		/*
		 * Potentially send a status message to the master
		 */
		if (still_sending && stream->standby_message_timeout > 0 &&
			feTimestampDifferenceExceeds(last_status, now,
										 stream->standby_message_timeout))
		{
			/* Time to send feedback! */
			if (!sendFeedback(conn, blockpos, now, false))
				goto error;
			last_status = now;
		}

		/*
		 * Calculate how long send/receive loops should sleep
		 */
		sleeptime = CalculateCopyStreamSleeptime(now, stream->standby_message_timeout,
												 last_status);

		r = CopyStreamReceive(conn, sleeptime, &copybuf);
		while (r != 0)
		{
			if (r == -1)
				goto error;
			if (r == -2)
			{
				PGresult   *res = HandleEndOfCopyStream(conn, stream, copybuf, blockpos, stoppos);

				if (res == NULL)
					goto error;
				else
					return res;
			}

			/* Check the message type. */
			if (copybuf[0] == 'k')
			{
				if (!ProcessKeepaliveMsg(conn, copybuf, r, blockpos,
										 &last_status))
					goto error;
			}
			else if (copybuf[0] == 'w')
			{
				if (!ProcessXLogDataMsg(conn, stream, copybuf, r, &blockpos))
					goto error;

				/*
				 * Check if we should continue streaming, or abort at this
				 * point.
				 */
				if (!CheckCopyStreamStop(conn, stream, blockpos, stoppos))
					goto error;
			}
			else
			{
				fprintf(stderr, _("%s: unrecognized streaming header: \"%c\"\n"),
						progname, copybuf[0]);
				goto error;
			}

			/*
			 * Process the received data, and any subsequent data we can read
			 * without blocking.
			 */
			r = CopyStreamReceive(conn, 0, &copybuf);
		}
	}

error:
	if (copybuf != NULL)
		PQfreemem(copybuf);
	return NULL;
}

/*
 * Wait until we can read CopyData message, or timeout.
 *
 * Returns 1 if data has become available for reading, 0 if timed out
 * or interrupted by signal, and -1 on an error.
 */
static int
CopyStreamPoll(PGconn *conn, long timeout_ms)
{
	int			ret;
	fd_set		input_mask;
	struct timeval timeout;
	struct timeval *timeoutptr;

	if (PQsocket(conn) < 0)
	{
		fprintf(stderr, _("%s: invalid socket: %s"), progname,
				PQerrorMessage(conn));
		return -1;
	}

	FD_ZERO(&input_mask);
	FD_SET(PQsocket(conn), &input_mask);

	if (timeout_ms < 0)
		timeoutptr = NULL;
	else
	{
		timeout.tv_sec = timeout_ms / 1000L;
		timeout.tv_usec = (timeout_ms % 1000L) * 1000L;
		timeoutptr = &timeout;
	}

	ret = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
	if (ret == 0 || (ret < 0 && errno == EINTR))
		return 0;				/* Got a timeout or signal */
	else if (ret < 0)
	{
		fprintf(stderr, _("%s: select() failed: %s\n"),
				progname, strerror(errno));
		return -1;
	}

	return 1;
}

/*
 * Receive CopyData message available from XLOG stream, blocking for
 * maximum of 'timeout' ms.
 *
 * If data was received, returns the length of the data. *buffer is set to
 * point to a buffer holding the received message. The buffer is only valid
 * until the next CopyStreamReceive call.
 *
 * 0 if no data was available within timeout, or wait was interrupted
 * by signal. -1 on error. -2 if the server ended the COPY.
 */
static int
CopyStreamReceive(PGconn *conn, long timeout, char **buffer)
{
	char	   *copybuf = NULL;
	int			rawlen;

	if (*buffer != NULL)
		PQfreemem(*buffer);
	*buffer = NULL;

	/* Try to receive a CopyData message */
	rawlen = PQgetCopyData(conn, &copybuf, 1);
	if (rawlen == 0)
	{
		/*
		 * No data available. Wait for some to appear, but not longer than the
		 * specified timeout, so that we can ping the server.
		 */
		if (timeout != 0)
		{
			int			ret;

			ret = CopyStreamPoll(conn, timeout);
			if (ret <= 0)
				return ret;
		}

		/* Else there is actually data on the socket */
		if (PQconsumeInput(conn) == 0)
		{
			fprintf(stderr,
					_("%s: could not receive data from WAL stream: %s"),
					progname, PQerrorMessage(conn));
			return -1;
		}

		/* Now that we've consumed some input, try again */
		rawlen = PQgetCopyData(conn, &copybuf, 1);
		if (rawlen == 0)
			return 0;
	}
	if (rawlen == -1)			/* end-of-streaming or error */
		return -2;
	if (rawlen == -2)
	{
		fprintf(stderr, _("%s: could not read COPY data: %s"),
				progname, PQerrorMessage(conn));
		return -1;
	}

	/* Return received messages to caller */
	*buffer = copybuf;
	return rawlen;
}

/**
* @brief Message delivery report callback.
*
* This callback is called exactly once per message, indicating if
* the message was succesfully delivered
* (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
* failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
*
* The callback is triggered from rd_kafka_poll() and executes on
* the application's thread.
*/
static void dr_msg_cb (rd_kafka_t *rk,
					   const rd_kafka_message_t *rkmessage, void *opaque)
{
	if (rkmessage->err)
		fprintf(stderr, "%% Message delivery failed: %s\n",
				rd_kafka_err2str(rkmessage->err));
	else
		fprintf(stderr,
				"%% Message delivered (%zd bytes, "
				"partition %"PRId32")\n",
				rkmessage->len, rkmessage->partition);

	/* The rkmessage is destroyed automatically by librdkafka */
}

/*
 * Process the keepalive message.
 */
static bool
ProcessKeepaliveMsg(PGconn *conn, char *copybuf, int len,
					XLogRecPtr blockpos, int64 *last_status)
{
	int			pos;
	bool		replyRequested;
	int64		now;

	/*
	 * Parse the keepalive message, enclosed in the CopyData message. We just
	 * check if the server requested a reply, and ignore the rest.
	 */
	pos = 1;					/* skip msgtype 'k' */
	pos += 8;					/* skip walEnd */
	pos += 8;					/* skip sendTime */

	if (len < pos + 1)
	{
		fprintf(stderr, _("%s: streaming header too small: %d\n"),
				progname, len);
		return false;
	}
	replyRequested = copybuf[pos];

	/* If the server requested an immediate reply, send one. */
	if (replyRequested && still_sending)
	{
		if (reportFlushPosition && lastFlushPosition < blockpos &&
			kafka_handle)
		{
			lastFlushPosition = blockpos;
		}

		now = feGetCurrentTimestamp();
		if (!sendFeedback(conn, blockpos, now, false))
			return false;
		*last_status = now;
	}

	return true;
}

/*
 * Process XLogData message.
 */
static bool
ProcessXLogDataMsg(PGconn *conn, StreamCtl *stream, char *copybuf, int len,
				   XLogRecPtr *blockpos)
{
	int			bytes_to_write;
	int			hdr_len;
	XLogRecPtr 	resume_lsn = InvalidXLogRecPtr;

	/*
	 * Once we've decided we don't want to receive any more, just ignore any
	 * subsequent XLogData messages.
	 */
	if (!(still_sending))
		return true;

	/*
	 * Read the header of the XLogData message, enclosed in the CopyData
	 * message. We only need the WAL location field (dataStart), the rest of
	 * the header is ignored.
	 */
	hdr_len = 1;				/* msgtype 'w' */
	hdr_len += 8;				/* dataStart */
	hdr_len += 8;				/* walEnd */
	hdr_len += 8;				/* sendTime */
	if (len < hdr_len)
	{
		fprintf(stderr, _("%s: streaming header too small: %d\n"),
				progname, len);
		return false;
	}
	*blockpos = fe_recvint64(&copybuf[1]);
	bytes_to_write = len - hdr_len;
	char    message[16 + bytes_to_write];

	memset(message, '\0', sizeof(message));
	printf("start lsn to send: %d\n", *blockpos);
	memcpy(message, blockpos, sizeof(blockpos));
	printf("len to send: %d\n", bytes_to_write);
	memcpy(message+8, &bytes_to_write, sizeof(bytes_to_write));
	memcpy(message+16, copybuf+hdr_len, bytes_to_write);

	// Open the data pipe
	if(!kafka_handle)
	{
		open_destination(stream);
	}


	write_destination(stream, message, 16+bytes_to_write);

	resume_lsn = *blockpos + bytes_to_write;
	fprintf(stderr,
			_("%s: wrote %u bytes to WAL file \"%s\": block pos: %u, start lsn: %u, next lsn: %u\n"),
			progname, bytes_to_write, stream->destination, *blockpos, fe_recvint64(&copybuf[1]), resume_lsn);

	// Update resume lsn
	// TODO: record the timeline as well. In the absence of recording timeline it assumes 1
	fseek(lsnfile, 0, SEEK_SET);
	fprintf(lsnfile, "%lu", resume_lsn);

	return true;
}

/*
 * Handle end of the copy stream.
 */
static PGresult *
HandleEndOfCopyStream(PGconn *conn, StreamCtl *stream, char *copybuf,
					  XLogRecPtr blockpos, XLogRecPtr *stoppos)
{
	PGresult   *res = PQgetResult(conn);

	/*
	 * The server closed its end of the copy stream.  If we haven't closed
	 * ours already, we need to do so now, unless the server threw an error,
	 * in which case we don't.
	 */
	if (still_sending)
	{
		if (!close_destination(stream, blockpos))
		{
			/* Error message written in close_destination() */
			PQclear(res);
			return NULL;
		}
		if (PQresultStatus(res) == PGRES_COPY_IN)
		{
			if (PQputCopyEnd(conn, NULL) <= 0 || PQflush(conn))
			{
				fprintf(stderr,
						_("%s: could not send copy-end packet: %s"),
						progname, PQerrorMessage(conn));
				PQclear(res);
				return NULL;
			}
			res = PQgetResult(conn);
		}
		still_sending = false;
	}
	if (copybuf != NULL)
		PQfreemem(copybuf);
	*stoppos = blockpos;
	return res;
}

/*
 * Check if we should continue streaming, or abort at this point.
 */
static bool
CheckCopyStreamStop(PGconn *conn, StreamCtl *stream, XLogRecPtr blockpos,
					XLogRecPtr *stoppos)
{
	if (still_sending && stream->stream_stop(blockpos, stream->timeline, false))
	{
		if (!close_destination(stream, blockpos))
		{
			/* Potential error message is written by close_destination */
			return false;
		}
		if (PQputCopyEnd(conn, NULL) <= 0 || PQflush(conn))
		{
			fprintf(stderr, _("%s: could not send copy-end packet: %s"),
					progname, PQerrorMessage(conn));
			return false;
		}
		still_sending = false;
	}

	return true;
}

/*
 * Calculate how long send/receive loops should sleep
 */
static long
CalculateCopyStreamSleeptime(int64 now, int standby_message_timeout,
							 int64 last_status)
{
	int64		status_targettime = 0;
	long		sleeptime;

	if (standby_message_timeout && still_sending)
		status_targettime = last_status +
			(standby_message_timeout - 1) * ((int64) 1000);

	if (status_targettime > 0)
	{
		long		secs;
		int			usecs;

		feTimestampDifference(now,
							  status_targettime,
							  &secs,
							  &usecs);
		/* Always sleep at least 1 sec */
		if (secs <= 0)
		{
			secs = 1;
			usecs = 0;
		}

		sleeptime = secs * 1000 + usecs / 1000;
	}
	else
		sleeptime = -1;

	return sleeptime;
}
