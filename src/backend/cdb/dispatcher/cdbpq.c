#include "postgres.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbpq.h"

int
PQsendGpQuery_shared(PGconn *conn, char *shared_query, int query_len, bool nonblock)
{
	int ret;

	if (!PQsendQueryStart(conn, true))
		return 0;

	if (!shared_query)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("shared mpp-query string is a null pointer\n"));
		return 0;
	}

	if (conn->outBuffer_shared)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("Cannot overwrite shared query string\n"));
		return 0;
	}

	/*
	 * Stash the original buffer and switch to the incoming pointer.
	 * We will restore the buffer after completing to send the shared query.
	 */
	conn->outBufferSaved = conn->outBuffer;

	conn->outBuffer = shared_query;
	conn->outBuffer_shared = true;

	conn->outMsgStart = 0;
	conn->outMsgEnd = query_len;
	conn->outCount = query_len;

	/*
	 * Register this command in libpq's command queue (added in PG14), marking
	 * it as a row-returning simple query.  Without a queue entry,
	 * getRowDescriptions() in fe-protocol3.c builds a PGRES_COMMAND_OK result
	 * instead of PGRES_TUPLES_OK, and every DataRow that a dispatched query
	 * returns (e.g. pg_highest_oid() OID sync, indcheckxmin sync, ANALYZE
	 * sampling) is rejected with "server sent data (D message) without prior
	 * row description (T message)", surfacing on the QD as "no primary message
	 * received".  This inlines pqAllocCmdQueueEntry()/pqAppendCmdQueueEntry(),
	 * which are static in fe-exec.c.
	 */
	{
		PGcmdQueueEntry *entry;

		if (conn->cmd_queue_recycle == NULL)
		{
			entry = (PGcmdQueueEntry *) malloc(sizeof(PGcmdQueueEntry));
			if (entry == NULL)
			{
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("out of memory\n"));
				return 0;
			}
		}
		else
		{
			entry = conn->cmd_queue_recycle;
			conn->cmd_queue_recycle = entry->next;
		}
		entry->next = NULL;
		entry->query = NULL;
		entry->queryclass = PGQUERY_SIMPLE;

		if (conn->cmd_queue_head == NULL)
			conn->cmd_queue_head = entry;
		else
			conn->cmd_queue_tail->next = entry;
		conn->cmd_queue_tail = entry;
	}

	/*
	 * Give the data a push.  In nonblock mode, don't complain if we're unable
	 * to send it all; PQgetResult() will do any additional flushing needed.
	 */
	if (nonblock)
		ret = pqFlushNonBlocking(conn);
	else
		ret = pqFlush(conn);

	if (ret < 0)
	{
		/* error message should be set up already */
		return 0;
	}

	/* OK, it's launched! */
	conn->asyncStatus = PGASYNC_BUSY;
	return 1;
}
