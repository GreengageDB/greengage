/*-------------------------------------------------------------------------
 *
 * explain_state.c
 *	  EXPLAIN options and state of extensions
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * This is the mechanism of PostgreSQL 18 for EXPLAIN options of
 * extensions.  Greengage keeps the rest of what PostgreSQL 18 moved into
 * a file of this name, NewExplainState and the parsing of the options,
 * in explain.c.
 *
 * In-core options have hard-coded fields inside ExplainState; e.g. if
 * the user writes EXPLAIN (BUFFERS) then ExplainState's "buffers" member
 * will be set to true. Extensions can also register options using
 * RegisterExtensionExplainOption; so that e.g. EXPLAIN (BICYCLE 'red')
 * will invoke a designated handler that knows what the legal values are
 * for the BICYCLE option. However, it's not enough for an extension to be
 * able to parse new options: it also needs a place to store the results
 * of that parsing, and an ExplainState has no 'bicycle' field.
 *
 * To solve this problem, an ExplainState can contain an array of opaque
 * pointers, one per extension. An extension can use GetExplainExtensionId
 * to acquire an integer ID to acquire an offset into this array that is
 * reserved for its exclusive use, and then use GetExplainExtensionState
 * and SetExplainExtensionState to read and write its own private state
 * within an ExplainState.
 *
 * Note that there is no requirement that the name of the option match
 * the name of the extension; e.g. a pg_explain_conveyance extension could
 * implement options for BICYCLE, MONORAIL, etc.
 *
 * IDENTIFICATION
 *	  src/backend/commands/explain_state.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain.h"
#include "commands/explain_state.h"
#include "utils/memutils.h"

/* Hook to perform additional EXPLAIN options validation */
explain_validate_options_hook_type explain_validate_options_hook = NULL;

typedef struct
{
	const char *option_name;
	ExplainOptionHandler option_handler;
} ExplainExtensionOption;

static const char **ExplainExtensionNameArray = NULL;
static int	ExplainExtensionNamesAssigned = 0;
static int	ExplainExtensionNamesAllocated = 0;

static ExplainExtensionOption *ExplainExtensionOptionArray = NULL;
static int	ExplainExtensionOptionsAssigned = 0;
static int	ExplainExtensionOptionsAllocated = 0;

/*
 * The smallest power of 2 that is at least n, for n > 0; PostgreSQL 18
 * uses pg_nextpower2_32, which PostgreSQL 12 does not have.
 */
static int
explain_next_power2(int n)
{
	int			p = 1;

	while (p < n)
		p <<= 1;
	return p;
}

/*
 * Map the name of an extension to an integer ID.
 *
 * Within the lifetime of a particular backend, the same name will be mapped
 * to the same ID every time. IDs are not stable across backends. Use the ID
 * that you get from this function to call GetExplainExtensionState and
 * SetExplainExtensionState.
 *
 * extension_name is assumed to be a constant string or allocated in storage
 * that will never be freed.
 */
int
GetExplainExtensionId(const char *extension_name)
{
	/* Search for an existing extension by this name; if found, return ID. */
	for (int i = 0; i < ExplainExtensionNamesAssigned; ++i)
		if (strcmp(ExplainExtensionNameArray[i], extension_name) == 0)
			return i;

	/* If there is no array yet, create one. */
	if (ExplainExtensionNameArray == NULL)
	{
		ExplainExtensionNamesAllocated = 16;
		ExplainExtensionNameArray = (const char **)
			MemoryContextAlloc(TopMemoryContext,
							   ExplainExtensionNamesAllocated
							   * sizeof(char *));
	}

	/* If there's an array but it's currently full, expand it. */
	if (ExplainExtensionNamesAssigned >= ExplainExtensionNamesAllocated)
	{
		int			i = explain_next_power2(ExplainExtensionNamesAssigned + 1);

		ExplainExtensionNameArray = (const char **)
			repalloc(ExplainExtensionNameArray, i * sizeof(char *));
		ExplainExtensionNamesAllocated = i;
	}

	/* Assign and return new ID. */
	ExplainExtensionNameArray[ExplainExtensionNamesAssigned] = extension_name;
	return ExplainExtensionNamesAssigned++;
}

/*
 * Get extension-specific state from an ExplainState.
 *
 * See comments for SetExplainExtensionState, below.
 */
void *
GetExplainExtensionState(ExplainState *es, int extension_id)
{
	Assert(extension_id >= 0);

	if (extension_id >= es->extension_state_allocated)
		return NULL;

	return es->extension_state[extension_id];
}

/*
 * Store extension-specific state into an ExplainState.
 *
 * To use this function, first obtain an integer extension_id using
 * GetExplainExtensionId. Then use this function to store an opaque pointer
 * in the ExplainState. Later, you can retrieve the opaque pointer using
 * GetExplainExtensionState.
 */
void
SetExplainExtensionState(ExplainState *es, int extension_id, void *opaque)
{
	Assert(extension_id >= 0);

	/* If there is no array yet, create one. */
	if (es->extension_state == NULL)
	{
		es->extension_state_allocated =
			Max(16, explain_next_power2(extension_id + 1));
		es->extension_state =
			palloc0(es->extension_state_allocated * sizeof(void *));
	}

	/* If there's an array but it's currently full, expand it. */
	if (extension_id >= es->extension_state_allocated)
	{
		int			i;

		i = explain_next_power2(extension_id + 1);
		es->extension_state = (void **)
			repalloc(es->extension_state, i * sizeof(void *));
		memset(es->extension_state + es->extension_state_allocated, 0,
			   (i - es->extension_state_allocated) * sizeof(void *));
		es->extension_state_allocated = i;
	}

	es->extension_state[extension_id] = opaque;
}

/*
 * Register a new EXPLAIN option.
 *
 * When option_name is used as an EXPLAIN option, handler will be called and
 * should update the ExplainState passed to it. See comments at top of file
 * for a more detailed explanation.
 *
 * option_name is assumed to be a constant string or allocated in storage
 * that will never be freed.
 */
void
RegisterExtensionExplainOption(const char *option_name,
							   ExplainOptionHandler handler)
{
	ExplainExtensionOption *exopt;

	/* Search for an existing option by this name; if found, update handler. */
	for (int i = 0; i < ExplainExtensionOptionsAssigned; ++i)
	{
		if (strcmp(ExplainExtensionOptionArray[i].option_name,
				   option_name) == 0)
		{
			ExplainExtensionOptionArray[i].option_handler = handler;
			return;
		}
	}

	/* If there is no array yet, create one. */
	if (ExplainExtensionOptionArray == NULL)
	{
		ExplainExtensionOptionsAllocated = 16;
		ExplainExtensionOptionArray = (ExplainExtensionOption *)
			MemoryContextAlloc(TopMemoryContext,
							   ExplainExtensionOptionsAllocated
							   * sizeof(ExplainExtensionOption));
	}

	/* If there's an array but it's currently full, expand it. */
	if (ExplainExtensionOptionsAssigned >= ExplainExtensionOptionsAllocated)
	{
		int			i = explain_next_power2(ExplainExtensionOptionsAssigned + 1);

		ExplainExtensionOptionArray = (ExplainExtensionOption *)
			repalloc(ExplainExtensionOptionArray, i * sizeof(ExplainExtensionOption));
		ExplainExtensionOptionsAllocated = i;
	}

	/* Assign and return new ID. */
	exopt = &ExplainExtensionOptionArray[ExplainExtensionOptionsAssigned++];
	exopt->option_name = option_name;
	exopt->option_handler = handler;
}

/*
 * Apply an EXPLAIN option registered by an extension.
 *
 * If no extension has registered the named option, returns false. Otherwise,
 * calls the appropriate handler function and then returns true.
 */
bool
ApplyExtensionExplainOption(ExplainState *es, DefElem *opt, ParseState *pstate)
{
	for (int i = 0; i < ExplainExtensionOptionsAssigned; ++i)
	{
		if (strcmp(ExplainExtensionOptionArray[i].option_name,
				   opt->defname) == 0)
		{
			ExplainExtensionOptionArray[i].option_handler(es, opt, pstate);
			return true;
		}
	}

	return false;
}
