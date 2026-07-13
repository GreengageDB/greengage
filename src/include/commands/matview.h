/*-------------------------------------------------------------------------
 *
 * matview.h
 *	  prototypes for matview.c.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "catalog/objectaddress.h"
#include "executor/execdesc.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/relcache.h"


extern void SetMatViewPopulatedState(Relation relation, bool newstate);

extern ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
										QueryCompletion *qc);
extern ObjectAddress RefreshMatViewByOid(Oid matviewOid, bool is_create, bool skipData,
										 bool concurrent, const char *queryString,
										 QueryCompletion *qc);

extern DestReceiver *CreateTransientRelDestReceiver(Oid transientoid, Oid oldreloid, bool concurrent,
													char relpersistence, bool skipdata);

extern bool MatViewIncrementalMaintenanceIsEnabled(void);

extern void transientrel_init(QueryDesc *queryDesc);

#endif							/* MATVIEW_H */
