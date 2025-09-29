#include <sys/stat.h>

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "common/relpath.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/relfilenodemap.h"
#include "catalog/pg_tablespace.h"

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

PG_FUNCTION_INFO_V1(gp_get_relfilenodes);
Datum gp_get_relfilenodes(PG_FUNCTION_ARGS)
{
	struct dirent   *direntry;
	user_fctx_data  *fctx_data;
	FuncCallContext *funcctx;

	if (SRF_IS_SQUELCH_CALL())
	{
		funcctx = SRF_PERCALL_SETUP();
		fctx_data = (user_fctx_data *) funcctx->user_fctx;
		goto srf_done;
	}

	Oid datoid = MyDatabaseId;
	Oid tablespace_oid = PG_GETARG_OID(0);

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

srf_done:
	FreeDir(fctx_data->dirdesc);
	SRF_RETURN_DONE(funcctx);
}
