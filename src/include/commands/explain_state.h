/*-------------------------------------------------------------------------
 *
 * explain_state.h
 *	  EXPLAIN options and state of extensions
 *
 * Greengage keeps ExplainState in explain.h.  This header declares the
 * part of PostgreSQL 18's explain_state.h that lets an extension
 * register its own EXPLAIN options and keep its own state in an
 * ExplainState, under the same names, so that an extension's code of
 * options and state builds against both Greengage and PostgreSQL 18.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/explain_state.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAIN_STATE_H
#define EXPLAIN_STATE_H

#include "commands/explain.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"

typedef void (*ExplainOptionHandler) (ExplainState *, DefElem *, ParseState *);

/* Hook to perform additional EXPLAIN options validation */
typedef void (*explain_validate_options_hook_type) (struct ExplainState *es, List *options,
													ParseState *pstate);
extern PGDLLIMPORT explain_validate_options_hook_type explain_validate_options_hook;

extern int	GetExplainExtensionId(const char *extension_name);
extern void *GetExplainExtensionState(ExplainState *es, int extension_id);
extern void SetExplainExtensionState(ExplainState *es, int extension_id,
									 void *opaque);

extern void RegisterExtensionExplainOption(const char *option_name,
										   ExplainOptionHandler handler);
extern bool ApplyExtensionExplainOption(ExplainState *es, DefElem *opt,
										ParseState *pstate);

#endif							/* EXPLAIN_STATE_H */
