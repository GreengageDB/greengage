//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		gpdbdefs.h
//
//	@doc:
//		C Linkage for GPDB functions used by GP optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDBDefs_H
#define GPDBDefs_H

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"

extern "C" {

#include "postgres.h"

#include "access/amapi.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_statistic_ext.h"
#include "cdb/cdbvars.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/hints.h"
#include "optimizer/orca.h"
#include "optimizer/plancat.h"
#include "optimizer/walkers.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "partitioning/partdesc.h"
#include "statistics/statistics.h"
#include "storage/proc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/syscache.h"
#if 0
#include "cdb/partitionselection.h"
#endif
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbmutate.h"
#include "cdb/cdbutil.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "parser/parse_coerce.h"
#include "tcop/dest.h"
#include "utils/elog.h"
#include "utils/faultinjector.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"
#include "utils/uri.h"

}  // end extern C

#pragma GCC diagnostic pop

#endif	// GPDBDefs_H

// EOF
