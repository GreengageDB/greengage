/*----------------------------------------------------------------------
 *
 * Greengage Database
 *
 * Copyright (c) 2025 Greengage Community
 *
 * Greengage Database is licensed under the Apache License Version 2.0
 * (see LICENSE & NOTICE) and is based on the PostgreSQL Database
 * Management System and Greenplum Database.
 * 
 *----------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "pgstat.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

static file_create_hook_type prev_file_create_hook = NULL;
static file_unlink_hook_type prev_file_unlink_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * RelFileNodeBackend-s for each temp table are stored in the list of arrays.
 * The list is located in shared memory. The head node of the list (TTSHeadNode)
 * is allocated using ShmemInitStruct. Next nodes (TTSNode) are allocated using
 * DSM. The next node is created when array of RelFileNodeBackend-s in
 * the previous one is full. It is assumed that array in the head node is large
 * enough to contain all RelFileNodeBackend-s in normal case and DSM is used
 * very rarely.
 */

typedef struct TTSNode
{
	dsm_handle	next;	/* Handle of DSM segment with the next node */
	int			num;	/* Number of elements in files */
	RelFileNodeBackend files[1000000];
}	TTSNode;

typedef struct TTSHeadNode
{
	LWLock		lock;
	TTSNode		node;
}	TTSHeadNode;

static TTSHeadNode *head = NULL; /* Head of the list */

/* Get next node by current one */
static TTSNode *
next_node(const TTSNode *node)
{
	dsm_segment *dsm_seg;

	if (node->next == DSM_HANDLE_INVALID)
		return NULL;

	dsm_seg = dsm_find_mapping(node->next);
	if (dsm_seg == NULL)
	{
		dsm_seg = dsm_attach(node->next);
		dsm_pin_mapping(dsm_seg);
	}

	return dsm_segment_address(dsm_seg);
}

/*
 * Returns the last node or a new node if the last one is full.
 * Returns NULL when no need to add rnode to the list.
 */
static TTSNode *
get_node_to_append(RelFileNodeBackend rnode)
{
	for (TTSNode *node = &head->node;; node = next_node(node))
	{
		/* Don't add rnode when it exists in the list of arrays */
		for (int i = 0; i < node->num; i++)
			if (RelFileNodeBackendEquals(rnode, node->files[i]))
				return NULL;

		if (node->next != DSM_HANDLE_INVALID)
			continue;

		/* Create a new node if the last node is full */
		if (node->num == ARRAY_SIZE(node->files))
		{
			dsm_segment *next_seg = dsm_create(sizeof(TTSNode));
			dsm_pin_mapping(next_seg);
			node->next = dsm_segment_handle(next_seg);
			node = dsm_segment_address(next_seg);
			node->next = DSM_HANDLE_INVALID;
			node->num = 0;
		}

		return node;
	}
}

/*
 * This function is called with the same argument when each fork is created.
 * Add file info to the list if it is not there.
 */
static void
tts_file_create_hook(RelFileNodeBackend rnode)
{
	TTSNode	   *node;

	if (prev_file_create_hook)
		(*prev_file_create_hook)(rnode);

	if (!RelFileNodeBackendIsTemp(rnode) || head == NULL)
		return;

	rnode.backend = MyBackendId;

	LWLockAcquire(&head->lock, LW_EXCLUSIVE);

	node = get_node_to_append(rnode);
	if (node != NULL)
		node->files[node->num++] = rnode;

	LWLockRelease(&head->lock);
}

static void
delete_from_ttsnode(TTSNode *node, int index, TTSNode *prev_node)
{
	/* Find the last node */
	TTSNode *last_node = node;
	TTSNode *last_prev_node = prev_node;

	while (last_node->next != DSM_HANDLE_INVALID)
	{
		last_prev_node = last_node;
		last_node = next_node(last_node);
	}

	/* replace the deleted element with the last one */
	node->files[index] = last_node->files[last_node->num - 1];

	if (last_node->num > 1)
		last_node->num--;
	else if (last_node == &head->node)
		head->node.num = 0;
	else
	{
		/*
		 * last_prev_node != NULL because last_node is not head.
		 * next_node() has been called, so the mapping exists.
		 */
		dsm_detach(dsm_find_mapping(last_prev_node->next));
		last_prev_node->next = DSM_HANDLE_INVALID;
	}
}

/* This function is called once for all forks. Delete file info from the list */
static void
tts_file_unlink_hook(RelFileNodeBackend rnode)
{
	if (prev_file_unlink_hook)
		(*prev_file_unlink_hook)(rnode);

	if (!RelFileNodeBackendIsTemp(rnode) || head == NULL)
		return;

	rnode.backend = MyBackendId;
	LWLockAcquire(&head->lock, LW_EXCLUSIVE);

	/* Find rnode in the list of arrays and delete it from the list node */
	for (TTSNode *node = &head->node, *prev_node = NULL;
		 node != NULL;
		 prev_node = node, node = next_node(node))
	{
		for (int i = 0; i < node->num; i++)
			if (RelFileNodeBackendEquals(rnode, node->files[i]))
			{
				delete_from_ttsnode(node, i, prev_node);
				goto lExit;
			}
	}

lExit:
	LWLockRelease(&head->lock);
}

/* Postmaster creates a new shared memory space for the head node of the list */
static void
tts_shmem_startup(void)
{
	bool		found;
	int			tranche_id;
	static LWLockTranche tranche;

	if (prev_shmem_startup_hook)
		(*prev_shmem_startup_hook)();

	head = ShmemInitStruct("temp_tables_stat", sizeof(TTSHeadNode), &found);
	if (found)
		return;

	tranche_id = LWLockNewTrancheId();
	tranche.name = "temp_tables_stat";
	tranche.array_base = &head->lock;
	tranche.array_stride = sizeof(head->lock);
	LWLockRegisterTranche(tranche_id, &tranche);
	LWLockInitialize(&head->lock, tranche_id);
	head->node.next = DSM_HANDLE_INVALID;
	head->node.num = 0;
}

/*
 * Get size of all files from the dirname directory, which names start
 * with fn_start
 */
static int64
tts_get_file_size(const char *dirname, const char *fn_start)
{
	struct dirent *direntry;
	int64		dirsize = 0;
	const size_t fn_start_len = strlen(fn_start);
	DIR		   *dirdesc = AllocateDir(dirname);

	if (!dirdesc)
		return 0;

	while ((direntry = ReadDir(dirdesc, dirname)) != NULL)
	{
		struct stat fst;
		char		fn[MAXPGPATH * 2];

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0 ||
			strncmp(direntry->d_name, fn_start, fn_start_len) != 0)
			continue;

		snprintf(fn, sizeof(fn), "%s/%s", dirname, direntry->d_name);

		if (stat(fn, &fst) < 0)
			continue;

		dirsize += fst.st_size;
	}

	FreeDir(dirdesc);
	return dirsize;
}

/* Copy the files info from the list to local memory */
static RelFileNodeBackend *
get_files(uint32 *files_num)
{
	RelFileNodeBackend *files;

	*files_num = 0;

	LWLockAcquire(&head->lock, LW_SHARED);

	/* Count files of temp tables */
	for (const TTSNode *node = &head->node; node != NULL; node = next_node(node))
		*files_num += node->num;

	/* Allocate local memory for array of the files data */
	files = palloc(sizeof(*files) * (*files_num));

	/* Combine arrays from the list nodes into one array */
	*files_num = 0;
	for (const TTSNode *node = &head->node; node != NULL; node = next_node(node))
	{
		RelFileNodeBackend *dst = files + (*files_num);
		memcpy(dst, node->files, sizeof(*files) * node->num);
		*files_num += node->num;
	}

	LWLockRelease(&head->lock);

	return files;
}

/* Get temp tables files list on segments */
PG_FUNCTION_INFO_V1(tts_get_seg_files);
Datum
tts_get_seg_files(PG_FUNCTION_ARGS)
{
	enum { NATTR = 5 };

	FuncCallContext *funcctx;
	const PgBackendStatus *beStatus;
	const RelFileNodeBackend *file;
	char	   *sep;
	char	   *path;
	HeapTuple	tuple;
	Datum		values[NATTR] = {0};
	bool		nulls [NATTR] = {0};
	static const PgBackendStatus *beStatuses = NULL;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(NATTR, false);
		TupleDescInitEntry(tupdesc, 1, "user_id", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "sess_id", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, 3, "path", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, 4, "content", INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, 5, "size", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		if (head->node.num == 0)
		{
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		funcctx->user_fctx = get_files(&funcctx->max_calls);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr >= funcctx->max_calls)
		SRF_RETURN_DONE(funcctx);

	if (beStatuses == NULL)
	{
		bool found;
		Size size = mul_size(sizeof(PgBackendStatus), MaxBackends);
		beStatuses = ShmemInitStruct("Backend Status Array", size, &found);
		if (!found)
			ereport(ERROR, (errmsg("Backend Status Array is not found")));
	}

	file = ((RelFileNodeBackend *) funcctx->user_fctx) + funcctx->call_cntr;

	beStatus = &beStatuses[file->backend - 1];
	values[0] = ObjectIdGetDatum(beStatus->st_userid);
	values[1] = Int32GetDatum(beStatus->st_session_id);
	path = relpathbackend(file->node, TempRelBackendId, MAIN_FORKNUM);
	values[2] = CStringGetTextDatum(path);
	values[3] = Int16GetDatum(GpIdentity.segindex);
	sep = strrchr(path, '/');
	Assert(sep != NULL);
	*sep = '\0';
	values[4] = Int64GetDatum(tts_get_file_size(path, sep + 1));
	pfree(path);

	tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
			(errmsg("temp_tables_stat is not in shared_preload_libraries")));
	}

	if (IS_QUERY_DISPATCHER())
		return;

	RequestAddinShmemSpace(sizeof(TTSHeadNode));

	prev_file_create_hook = file_create_hook;
	file_create_hook = tts_file_create_hook;

	prev_file_unlink_hook = file_unlink_hook;
	file_unlink_hook = tts_file_unlink_hook;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = tts_shmem_startup;
}

void
_PG_fini(void)
{
	if (IS_QUERY_DISPATCHER())
		return;

	file_create_hook = prev_file_create_hook;
	file_unlink_hook = prev_file_unlink_hook;
	shmem_startup_hook = prev_shmem_startup_hook;
}
