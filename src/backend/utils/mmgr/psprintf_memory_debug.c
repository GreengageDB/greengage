#include "utils/palloc_memory_debug_undef.h"

char *
_psprintf(const char *func, const char *file, int LINE, const char *fmt, ...)
{
	size_t		len = 128;		/* initial assumption about buffer size */

	for (;;)
	{
		char	   *result;
		va_list		args;
		size_t		newlen;

		/*
		 * Allocate result buffer.  Note that in frontend this maps to malloc
		 * with exit-on-error.
		 */
		result = (char *) _palloc(len, func, file, LINE);

		/* Try to format the data. */
		va_start(args, fmt);
		newlen = pvsnprintf(result, len, fmt, args);
		va_end(args);

		if (newlen < len)
			return result;		/* success */

		/* Release buffer and loop around to try again with larger len. */
		pfree(result);
		len = newlen;
	}
}
