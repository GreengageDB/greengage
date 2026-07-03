#include "postgres.h"

#include "parse_keyword.h"

/*
 * PG16 relocated gramparse.h to backend-private src/backend/parser and it is
 * no longer on the contrib include path.  orafce only needs the keyword-lookup
 * API from it: ScanKeywordTokens is declared in parser/scanner.h, while the
 * ScanKeywords list plus ScanKeywordLookup/GetScanKeyword come from
 * common/keywords.h (included below and pulled in transitively by scanner.h).
 */
#if PG_VERSION_NUM >= 90600
#include "parser/scanner.h"
#else
#include "parser/gramparse.h"
#endif

#if PG_VERSION_NUM >= 90600

#include "common/keywords.h"

#else

#include "parser/keywords.h"

#endif

#if PG_VERSION_NUM >= 120000

const char *
orafce_scan_keyword(const char *text, int *keycode)
{
	int		kwnum;

	kwnum = ScanKeywordLookup(text, &ScanKeywords);
	if (kwnum >= 0)
	{
		*keycode = ScanKeywordTokens[kwnum];
		return GetScanKeyword(kwnum, &ScanKeywords);
	}

	return NULL;
}

#else

const char *
orafce_scan_keyword(const char *text, int *keycode)
{
	const ScanKeyword *keyword;

	keyword = ScanKeywordLookup(text, ScanKeywords, NumScanKeywords);
	if (keyword)
	{
		*keycode = keyword->value;
		return keyword->name;
	}

	return NULL;
}

#endif
