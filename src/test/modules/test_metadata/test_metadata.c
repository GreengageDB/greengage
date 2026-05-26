#include "postgres.h"
#include "fmgr.h"
#include "libpq/pqcomm.h"
#include "utils/builtins.h"
#include "libpq/libpq.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbconn.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_create_metadata_queue);
PG_FUNCTION_INFO_V1(test_delete_metadata_queue);
PG_FUNCTION_INFO_V1(test_send_empty_metadata);
PG_FUNCTION_INFO_V1(test_send_metadata);
PG_FUNCTION_INFO_V1(test_check_metadata);
PG_FUNCTION_INFO_V1(test_count_metadata);
PG_FUNCTION_INFO_V1(test_clean_metadata);

Datum
test_create_metadata_queue(PG_FUNCTION_ARGS)
{
	ggMetadataQueueId queue_id = PQMetadataNextQueueId();

	PQCreateMetadataQueue(queue_id);

	PG_RETURN_INT32(queue_id);
}

Datum
test_delete_metadata_queue(PG_FUNCTION_ARGS)
{
	ggMetadataQueueId queue_id = PG_GETARG_INT32(0);

	PQDeleteMetadataQueue(queue_id);

	PG_RETURN_INT32(0);
}

Datum
test_send_empty_metadata(PG_FUNCTION_ARGS)
{
	ggMetadataQueueId queue_id = PG_GETARG_INT32(0);

	elog(WARNING, "Sending empty metadata...");
	pq_metadatasend("", 0, queue_id);
	elog(WARNING, "Empty metadata sent!");
	PG_RETURN_INT32(0);
}

Datum
test_send_metadata(PG_FUNCTION_ARGS)
{
	uint32		len = PG_GETARG_UINT32(0);
	uint32		id = PG_GETARG_UINT32(1);
	ggMetadataQueueId queue_id = PG_GETARG_INT32(2);

	char	   *metadata = palloc(len);

	if (len > sizeof(uint32))
	{
		metadata[0] = id % 256;

		for (int i = 1; i < len; i++)
		{
			metadata[i] = (len + id - i) % 255;
		}
	}

	/*
	 * Send custom metadata, do not log when wrong queue test performed,
	 * to not clutter output and deal with output lines ordering.
	 */
	if (queue_id != 10000)
		elog(WARNING, "Sending custom metadata...");
	pq_metadatasend(metadata, len, queue_id);
	if (queue_id != 10000)
		elog(WARNING, "Custom metadata sent!");

	pfree(metadata);

	/* Return a simple result */
	PG_RETURN_INT32(len);
}

Datum
test_check_metadata(PG_FUNCTION_ARGS)
{
	Assert(Gp_role == GP_ROLE_DISPATCH);
	ggMetadataQueueId queue_id = PG_GETARG_INT32(0);

	/*
	 * Metadata records are unordered for the each query, so to make
	 * predictable results, make them in few passes
	 */

	int			numsegments = getgpsegmentCount();
	int			count = 0;

	for (int seg_id = -1; seg_id < numsegments; seg_id++)
	{
		ggMetadataChunkIterator it = PQMetadataWalk(queue_id);

		for (; it; it = PQgetNextMetadata(it))
		{
			int32		id = -1;
			ggMetadataDescriptor metadata;

			PQgetMetadata(it, &metadata);

			if (metadata.metadataLen > 0)
			{
				id = ((char *)metadata.data)[0];

				Assert(id == metadata.segindex);

				for (int i = 1; (id == seg_id) && (i < metadata.metadataLen); i++)
				{
					char		expected = (metadata.metadataLen + id - i) % 255;

					if (((char *)metadata.data)[i] != expected)
						elog(ERROR, "Metadata is BAD");
				}
			}

			if (seg_id == id)
			{
				elog(WARNING, "Custom metadata received, len=%d, id=%d", metadata.metadataLen, id);
				count++;
			}
		}

	}
	PG_RETURN_INT32(count);
}

Datum
test_count_metadata(PG_FUNCTION_ARGS)
{
	ggMetadataQueueId queue_id = PG_GETARG_INT32(0);
	int			count = PQgetMetadataCount(queue_id);

	PG_RETURN_INT32(count);
}

Datum
test_clean_metadata(PG_FUNCTION_ARGS)
{
	ggMetadataQueueId queue_id = PG_GETARG_INT32(0);

	PQCleanMetadata(queue_id);
	PG_RETURN_INT32(0);
}
