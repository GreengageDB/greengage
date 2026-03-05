#include "postgres.h"
#include "fmgr.h"
#include "libpq/pqcomm.h"
#include "utils/builtins.h"
#include "libpq/libpq.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_send_empty_metadata);
PG_FUNCTION_INFO_V1(test_send_metadata);
PG_FUNCTION_INFO_V1(test_check_metadata);
PG_FUNCTION_INFO_V1(test_clean_metadata);


Datum
test_send_empty_metadata(PG_FUNCTION_ARGS)
{
    elog(WARNING, "Sending empty metadata...");
    pq_metadatasend("", 0);
    elog(WARNING, "Empty metadata sent!");
    PG_RETURN_INT32(0);
}

Datum
test_send_metadata(PG_FUNCTION_ARGS)
{
    uint32		len = PG_GETARG_UINT32(0);
    
    char *metadata = palloc(len);

    for (int i = 0; i < len; i++) 
    {
        metadata[i] = (len - i) % 255;
    }
    
    /* Send custom metadata */
    elog(WARNING, "Sending custom metadata...");
    pq_metadatasend(metadata, len);
    elog(WARNING, "Custom metadata sent!");
    
    pfree(metadata);

    /* Return a simple result */
    PG_RETURN_INT32(len);
}

Datum test_check_metadata(PG_FUNCTION_ARGS)
{
    ggMetadataChunkIterator it = PQMetadataWalk();
    int count = 0;

    void *metadata;
    int length;

    for (; it; it = PQgetNextMetadata(it))
    {
        PQgetMetadata(it, &length, &metadata);

        elog(WARNING, "Custom metadata received, len=%d", length);
        for (int i = 0; i < length; i++)
        {
            char expected = (length - i) % 255;

            if (((char *)metadata)[i] != expected) 
                elog(ERROR, "Metadata is BAD");
        }

        count++;
    }
    PG_RETURN_INT32(count);
}

Datum test_clean_metadata(PG_FUNCTION_ARGS)
{
    PQCleanMetadata();
    PG_RETURN_INT32(0);
}
