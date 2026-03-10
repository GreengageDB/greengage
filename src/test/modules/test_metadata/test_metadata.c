#include "postgres.h"
#include "fmgr.h"
#include "libpq/pqcomm.h"
#include "utils/builtins.h"
#include "libpq/libpq.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_send_empty_metadata);
PG_FUNCTION_INFO_V1(test_send_metadata);
PG_FUNCTION_INFO_V1(test_check_metadata);
PG_FUNCTION_INFO_V1(test_count_metadata);
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
    uint32		id = PG_GETARG_UINT32(1);
    
    char *metadata = palloc(len);

    if (len > sizeof(uint32))
    {
        metadata[0] = id % 256;

        for (int i = 1; i < len; i++) 
        {
            metadata[i] = (len + id - i) % 255;
        }
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
    Assert(Gp_role == GP_ROLE_DISPATCH);

    // Metadata records are unordered for the each query, so to make predictable
    // records, make them in few passes

    int numsegments = getgpsegmentCount();
    int count = 0;

    for (int seg_id = -1; seg_id < numsegments; seg_id++)
    {
        ggMetadataChunkIterator it = PQMetadataWalk();        
        void *metadata;
        int length;

        for (; it; it = PQgetNextMetadata(it))
        {
            int32 id = -1;
            PQgetMetadata(it, &length, &metadata);

            if (length > 0)
            {
                id = ((char *)metadata)[0];
            
                for (int i = 1; (id == seg_id) && (i < length); i++)
                {
                    char expected = (length + id - i) % 255;

                    if (((char *)metadata)[i] != expected) 
                        elog(ERROR, "Metadata is BAD");
                }
            }

            if (seg_id == id)
            {   
                elog(WARNING, "Custom metadata received, len=%d, id=%d", length, id);
                count++;
            }
        }
    
    }
   PG_RETURN_INT32(count);
}

Datum test_count_metadata(PG_FUNCTION_ARGS)
{
    int count = PQgetMetadataCount();
    PG_RETURN_INT32(count);
}

Datum test_clean_metadata(PG_FUNCTION_ARGS)
{
    PQCleanMetadata();
    PG_RETURN_INT32(0);
}
