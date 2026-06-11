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
	RelFileNodeBackend  rnodebackend;

	rnodebackend.node.spcNode = PG_GETARG_OID(0);
	rnodebackend.node.dbNode  = PG_GETARG_OID(1);
	rnodebackend.node.relNode = PG_GETARG_OID(2);

	rnodebackend.backend = InvalidBackendId; /* not temporary/local */

	DropRelFileNodeBuffers(smgropen(rnodebackend.node, rnodebackend.backend,
									SMGR_MD),
						   &fork, 1, &block);

	PG_RETURN_BOOL(true);
}

