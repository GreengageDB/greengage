#include "postgres.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum invalidate_buffers(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(invalidate_buffers);
Datum
invalidate_buffers(PG_FUNCTION_ARGS)
{
	BlockNumber	block = 0;
	ForkNumber	fork = MAIN_FORKNUM;
	RelFileNode	rnode;
	SMgrRelation smgr_reln;

	rnode.spcNode = PG_GETARG_OID(0);
	rnode.dbNode  = PG_GETARG_OID(1);
	rnode.relNode = PG_GETARG_OID(2);

	/* not temporary/local */
	smgr_reln = smgropen(rnode, InvalidBackendId, SMGR_MD);

	DropRelFileNodeBuffers(smgr_reln, &fork, 1, &block);

	PG_RETURN_BOOL(true);
}

