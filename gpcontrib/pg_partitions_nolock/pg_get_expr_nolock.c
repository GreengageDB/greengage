/*
 * Implementation of pg_get_expr without locks.
 */

/* .c include is intentional */
#include "../../src/backend/utils/adt/ruleutils.c"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


PG_FUNCTION_INFO_V1(pg_get_expr_nolock);
/*
 * This method is the same as the original 'pg_get_expr', but does not accept
 * relation ID, and uses 'InvalidOid' in pg_get_expr_worker instead.
 * 
 * As a result, it does not take locks on any tables. This is the origin of its
 * name.
 */
Datum
pg_get_expr_nolock(PG_FUNCTION_ARGS)
{
	text	   *expr = PG_GETARG_TEXT_P(0);
	int			prettyFlags;
	text		*result;

	prettyFlags = PRETTYFLAG_INDENT;

	result = pg_get_expr_worker(expr, InvalidOid, NULL, prettyFlags);

	PG_RETURN_TEXT_P(result);
}
