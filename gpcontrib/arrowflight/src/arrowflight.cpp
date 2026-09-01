/*-------------------------------------------------------------------------
 *
 * arrowflight.cpp
 *    Extension entry points for the experimental Arrow Flight integration.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(arrowflight_build_info);

Datum arrowflight_build_info(PG_FUNCTION_ARGS);

}

extern "C" Datum
arrowflight_build_info(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(af_arrow_build_info()));
}
