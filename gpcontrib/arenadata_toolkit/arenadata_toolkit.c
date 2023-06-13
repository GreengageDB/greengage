#include <dirent.h>
#include <sys/stat.h>
#include <ctype.h>
#include <stdlib.h>

#include "postgres.h"

#include "access/aomd.h"
#include "access/heapam.h"
#include "cdb/cdbvars.h"
#include "common/relpath.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "nodes/execnodes.h"
#include "cdb/cdbvars.h"
#include "utils/builtins.h"
#include "utils/relfilenodemap.h"
#include "utils/timestamp.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "catalog/pg_tablespace.h"
#include "storage/lock.h"

PG_MODULE_MAGIC;

/*
 * MAXPATHLEN_WITHSEGNO: size of path buffer for relation segment
 * ('segno' may require 12 additional bytes).
 */
#define MAXPATHLEN_WITHSEGNO (MAXPGPATH + 12)

static int64 calculate_relation_size(Relation rel, ForkNumber forknum);
static int64 get_heap_storage_total_bytes(Relation rel,
							 ForkNumber forknum, char *relpath);
static int64 get_ao_storage_total_bytes(Relation rel, char *relpath);
static bool calculate_ao_storage_perSegFile(const int segno, void *ctx);
static void fill_relation_seg_path(char *buf, int bufLen,
					   const char *relpath, int segNo);

/*
 * Structure used to accumulate the size of AO/CO relation from callback.
 */
struct calculate_ao_storage_callback_ctx
{
	char	   *relfilenode_path;
	int64		total_size;
};

/*
 * Function to calculate size of a relation by it's OID and optional forkNumber
 * (by default it's MAIN). The implementation of function is based on
 * pg_relation_size from dbsize.c
 */
PG_FUNCTION_INFO_V1(adb_relation_storage_size);
Datum
adb_relation_storage_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	text	   *forkName = PG_GETARG_TEXT_P(1);
	ForkNumber	forkNumber;
	Relation	rel;
	int64		size = 0;

	rel = try_relation_open(relOid, AccessShareLock, false);
	if (rel == NULL)
		PG_RETURN_NULL();

	forkNumber = forkname_to_number(text_to_cstring(forkName));

	if (relOid == 0 || rel->rd_node.relNode == 0)
		size = 0;
	else
		size = calculate_relation_size(rel, forkNumber);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql = psprintf(
			  "select arenadata_toolkit.adb_relation_storage_size(%u, '%s')",
								   relOid, forkNames[forkNumber]
		);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * Function calculates the size of a relation (one fork of this relation)
 *
 * This function must preserve the behaviour of the eponymous function from
 * dbsize.c. Thus calculation of size for heap/AO/CO relations is supported
 * (AO/CO relations don't have any extra forks, so only main fork is supported)
 * In other cases zero value is returned.
 */
static int64
calculate_relation_size(Relation rel, ForkNumber forknum)
{
	bool		isAOMainFork = RelationIsAppendOptimized(rel) && forknum == MAIN_FORKNUM;

	if (!(RelationIsHeap(rel) || isAOMainFork))
		return 0;

	char	   *relpath = relpathbackend(rel->rd_node, rel->rd_backend, forknum);

	if (RelationIsHeap(rel))
		return get_heap_storage_total_bytes(rel, forknum, relpath);

	return get_ao_storage_total_bytes(rel, relpath);
}

static void
fill_relation_seg_path(char *buf, int bufLen, const char *relpath, int segNo)
{
	if (segNo == 0)
		snprintf(buf, bufLen, "%s", relpath);
	else
		snprintf(buf, bufLen, "%s.%u", relpath, segNo);
}

static bool
calculate_ao_storage_perSegFile(const int segno, void *ctx)
{
	struct stat fst;
	char		segPath[MAXPATHLEN_WITHSEGNO];
	struct calculate_ao_storage_callback_ctx *calcCtx = ctx;

	CHECK_FOR_INTERRUPTS();

	fill_relation_seg_path(segPath, MAXPATHLEN_WITHSEGNO,
						   calcCtx->relfilenode_path, segno);

	if (stat(segPath, &fst) < 0)
	{
		if (errno == ENOENT)
			return false;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not access file %s: %m", segPath)));
	}
	calcCtx->total_size += fst.st_size;

	return true;
}

/*
 * Function calculates the size of heap tables.
 *
 * The code is based on calculate_relation_size from dbsize.c
 */
static int64
get_heap_storage_total_bytes(Relation rel, ForkNumber forknum, char *relpath)
{
	int64		totalsize = 0;
	char		segPath[MAXPATHLEN_WITHSEGNO];

	/*
	 * Ordinary relation, including heap and index. They take form of
	 * relationpath, or relationpath.%d There will be no holes, therefore, we
	 * can stop when we reach the first non-existing file.
	 */
	for (int segno = 0;; segno++)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		fill_relation_seg_path(segPath, MAXPATHLEN_WITHSEGNO, relpath, segno);
		if (stat(segPath, &fst) < 0)
		{
			if (errno == ENOENT)
				break;
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not stat file %s: %m", segPath)));
		}
		totalsize += fst.st_size;
	}

	return totalsize;
}

/*
 * Function calculates the size of AO/CO tables.
 */
static int64
get_ao_storage_total_bytes(Relation rel, char *relpath)
{
	struct calculate_ao_storage_callback_ctx ctx = {
		.relfilenode_path = relpath,
		.total_size = 0
	};

	/*
	 * ao_foreach_extent_file starts execution of callback for relfilenode
	 * file with extension 1 (segno=1) and ignores relfilenode file without
	 * extension (segno=0), which may be not empty (in case of utility
	 * operations (for ex: CTAS) zero segment will store tuples). Thus
	 * calculate segno=0 manually.
	 */
	(void) calculate_ao_storage_perSegFile(0, &ctx);

	ao_foreach_extent_file(calculate_ao_storage_perSegFile, &ctx);
	return ctx.total_size;
}

typedef struct
{
	char     *datpath;
	DIR      *dirdesc;
	TupleDesc tupdesc;
} user_fctx_data;

/*
 * Name of file must be "XXX.X" or "XXX"
 * where XXX is Oid. OID must be not more than OID_MAX.
 */
static Oid get_oid_from_filename(const char *filename)
{
	unsigned long int oid, segment;
	char trailer;

	int count = sscanf(filename, "%lu.%lu%c", &oid, &segment, &trailer);
	if (count < 1 || count > 2)
		return InvalidOid;
	if (oid > OID_MAX)
		return InvalidOid;

	return (Oid) oid;
}

PG_FUNCTION_INFO_V1(adb_get_relfilenodes);
Datum adb_get_relfilenodes(PG_FUNCTION_ARGS)
{
	Oid              datoid = MyDatabaseId;
	Oid              tablespace_oid = PG_GETARG_OID(0);

	struct dirent   *direntry;
	user_fctx_data  *fctx_data;
	FuncCallContext *funcctx;

	if (tablespace_oid == GLOBALTABLESPACE_OID)
		datoid = 0;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		fctx_data = palloc(sizeof(user_fctx_data));
		fctx_data->datpath = GetDatabasePath(datoid, tablespace_oid);
		fctx_data->dirdesc = AllocateDir(fctx_data->datpath);

		if (!fctx_data->dirdesc)
		{
			/* Nothing to do: empty tablespace (maybe it has been just created)*/
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		if (get_call_result_type(fcinfo, NULL, &fctx_data->tupdesc)
				!= TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("return type must be a row type")));

		funcctx->attinmeta = TupleDescGetAttInMetadata(fctx_data->tupdesc);
		funcctx->user_fctx = fctx_data;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	fctx_data = (user_fctx_data *) funcctx->user_fctx;

	while ((direntry = ReadDir(fctx_data->dirdesc, fctx_data->datpath)) != NULL)
	{
		struct stat fst;
		Datum       values[10];
		bool        nulls[10];
		char       *filename;
		Oid         reloid;
		Oid         relfilenode_oid;
		HeapTuple   tuple;

		CHECK_FOR_INTERRUPTS();

		if (direntry->d_type == DT_DIR)
			continue;

		relfilenode_oid = get_oid_from_filename(direntry->d_name);
		if (relfilenode_oid == InvalidOid)
			continue;

		filename = psprintf("%s/%s", fctx_data->datpath, direntry->d_name);

		if (stat(filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							filename)));
		}

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int16GetDatum(GpIdentity.segindex);
		values[1] = Int16GetDatum(GpIdentity.dbid);
		values[2] = DatumGetObjectId(datoid);
		values[3] = DatumGetObjectId(tablespace_oid);
		values[4] = CStringGetTextDatum(filename);

		reloid = RelidByRelfilenode(tablespace_oid, relfilenode_oid);
		if (OidIsValid(reloid))
		{
			values[5] = DatumGetObjectId(relfilenode_oid);
			values[6] = DatumGetObjectId(reloid);
		}
		else
		{
			nulls[5] = true;
			nulls[6] = true;
		}

		values[7] = Int64GetDatum(fst.st_size);
		values[8] = TimestampGetDatum(time_t_to_timestamptz(fst.st_mtime));
		values[9] = TimestampGetDatum(time_t_to_timestamptz(fst.st_ctime));

		tuple = heap_form_tuple(fctx_data->tupdesc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	FreeDir(fctx_data->dirdesc);
	SRF_RETURN_DONE(funcctx);
}

