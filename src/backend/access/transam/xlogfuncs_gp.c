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

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "utils/faultinjector.h"

#include "cdb/cdbutil.h"
#include "cdb/cdbdispatchresult.h"
#include "libpq-fe.h"
#include "funcapi.h"

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

	FuncCallContext *funcctx;
	Context *context;

	if(SRF_IS_FIRSTCALL())
	{
		TupleDesc     tupdesc;
		MemoryContext oldcontext;
		text          *restore_name = PG_GETARG_TEXT_P(0);
		char          *restore_name_str;
		char          *restore_command;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context for appropriate multiple function call */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* create tupdesc for result */
		tupdesc = CreateTemplateTupleDesc(2, false);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) 1,
						   "content_id",
						   INT2OID,
						   -1,
						   0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) 2,
						   "restore_lsn",
						   CSTRINGOID,
						   -1,
						   0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		context = (Context *) palloc0(sizeof(Context));
		context->cdb_pgresults.pg_results =
			(struct pg_result **) palloc(getgpsegmentCount() * sizeof(struct pg_result *));
		context->current_index = -1;

		restore_name_str = text_to_cstring(restore_name);

		if (!IS_QUERY_DISPATCHER())
			elog(ERROR,
				 "cannot use gp_create_restore_point() when not in QD mode");

		restore_command =
			psprintf("SELECT pg_catalog.pg_create_restore_point(%s)",
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
		context->qd_restorepoint_lsn = DirectFunctionCall1(pg_create_restore_point,
							PointerGetDatum(restore_name));

		LWLockRelease(TwophaseCommitLock);

		pfree(restore_command);

		funcctx->user_fctx = (void *) context;
		MemoryContextSwitchTo(oldcontext);
	}

	/* Using SRF to return all the segment LSN information of the form {SEGMENT_ID, LSN} */
	funcctx = SRF_PERCALL_SETUP();
	context = (Context *) funcctx->user_fctx;

	while (context->current_index < context->cdb_pgresults.numResults)
	{
		Datum           values[2];
		bool            nulls[2];
		HeapTuple       tuple;
		Datum           result;
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

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = Int16GetDatum(context->current_index);
		values[1] = CStringGetDatum(lsn_value);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		context->current_index++;

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}
