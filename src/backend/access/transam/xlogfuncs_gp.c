/*-------------------------------------------------------------------------
 *
 * xlogfuncs_gp.c
 *
 * GPDB-specific transaction log manager user interface functions
 *
 * This file contains WAL control and information functions.
 *
 * Portions Copyright (c) 2017-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog_fn.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "libpq-fe.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbdispatchresult.h"
#include "utils/faultinjector.h"

/*
 * gp_create_restore_point: a distributed named point for cluster restore
 */
Datum
gp_create_restore_point(PG_FUNCTION_ARGS)
{
	typedef struct Context
	{
		CdbPgResults cdb_pgresults;
		XLogRecPtr qd_restorepoint_lsn;
		int current_index;
	} Context;

	Context *context;
	text *restore_name = PG_GETARG_TEXT_P(0);
	char *restore_name_str;
	char *restore_command;
	int	num_segments;

	restore_name_str = text_to_cstring(restore_name);
	num_segments = getgpsegmentCount();

	context = (Context *) palloc0(sizeof(Context));
	context->cdb_pgresults.pg_results =
		(struct pg_result **) palloc(num_segments * sizeof(struct pg_result *));
	context->current_index = -1;

	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "cannot use gp_create_restore_point() when not in QD mode");

	restore_command = psprintf("SELECT pg_catalog.pg_create_restore_point(%s)",
								   quote_literal_cstr(restore_name_str));

	/*
	 * Acquire TwophaseCommitLock in EXCLUSIVE mode. This is to ensure
	 * cluster-wide restore point consistency by blocking distributed commit
	 * prepared broadcasts from concurrent twophase transactions where a QE
	 * segment has written WAL.
	 */
	LWLockAcquire(TwophaseCommitLock, LW_EXCLUSIVE);

	SIMPLE_FAULT_INJECTOR("gp_create_restore_point_acquired_lock");

	CdbDispatchCommand(restore_command,
					   DF_NEED_TWO_PHASE | DF_CANCEL_ON_ERROR,
					   &context->cdb_pgresults);
	context->qd_restorepoint_lsn = DirectFunctionCall1(pg_create_restore_point, PointerGetDatum(restore_name));

	LWLockRelease(TwophaseCommitLock);

	pfree(restore_command);

	// Prepare return value of LSNs
	StringInfoData result_str;
	initStringInfo(&result_str);

	appendStringInfoChar(&result_str, '{');
	while (context->current_index < context->cdb_pgresults.numResults)
	{
		char *lsn_value;
		if (context->current_index == -1)
		{
			lsn_value = psprintf("%X/%X",
								 (uint32) (context->qd_restorepoint_lsn >> 32), (uint32) context->qd_restorepoint_lsn);
		}
		else
		{
			struct pg_result *pgresult = context->cdb_pgresults.pg_results[context->current_index];
			lsn_value = PQgetvalue(pgresult, 0, 0);
		}

		appendStringInfo(&result_str, "{%d,%s},", context->current_index, lsn_value);
		context->current_index++;
	}
	result_str.data[result_str.len - 1] = '}';
	PG_RETURN_TEXT_P(cstring_to_text(result_str.data));
}
