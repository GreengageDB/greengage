/*
 * File hooks to track events.
 */

#include "file_hook.h"

#include "postgres.h"
#include "storage/smgr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/objectaccess.h"

#include "tf_shmem.h"
#include "gg_tables_tracking_guc.h"
#include "drops_track.h"

static file_create_hook_type next_file_create_hook = NULL;
static file_extend_hook_type next_file_extend_hook = NULL;
static file_truncate_hook_type next_file_truncate_hook = NULL;
static file_unlink_hook_type next_file_unlink_hook = NULL;

static bloom_t *non_committed_bloom = NULL;
static Oid	non_committed_dbid = InvalidOid;

static bool
is_file_node_trackable(RelFileNodeBackend *rnode)
{
	return !(rnode->node.dbNode == InvalidOid);
}

static void
file_node_set(RelFileNodeBackend *rnode)
{
	if (!is_file_node_trackable(rnode))
		return;

	bloom_set_set(rnode->node.dbNode, rnode->node.relNode);
}

/*
 * 'create' events stored in local bloom and merged only on commit, when
 * changes are already in catalog.
 */
static void
xact_end_create_callback(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT)
		return;

	ereport(DEBUG1, (errmsg("xact_end_create_callback")));

	if (event == XACT_EVENT_COMMIT)
		bloom_set_merge(non_committed_dbid, non_committed_bloom);

	pfree(non_committed_bloom);
	non_committed_bloom = NULL;
	non_committed_dbid = InvalidOid;
}

/*
 * Since we can't be sure that created rnode will be commited, the create events
 * are stored in a separate bloom filter.
 */
static void
hook_create(RelFileNodeBackend rnode)
{
	if (next_file_create_hook)
		next_file_create_hook(rnode);

	if (!is_file_node_trackable(&rnode))
		return;

	if (!non_committed_bloom)
	{
		non_committed_bloom =
			MemoryContextAlloc(TopMemoryContext, full_bloom_size(bloom_size));
		bloom_init(bloom_size, non_committed_bloom);
		non_committed_dbid = rnode.node.dbNode;
		RegisterXactCallbackOnce(xact_end_create_callback, NULL);
	}

	ereport(DEBUG1, (errmsg("hook_create: %d %d %d %d",
		 rnode.backend, rnode.node.dbNode,
		 rnode.node.spcNode, rnode.node.relNode)));

	bloom_set_bits(non_committed_bloom, rnode.node.relNode);

}

static void
hook_extend(RelFileNodeBackend rnode)
{
	if (next_file_extend_hook)
		next_file_extend_hook(rnode);

	ereport(DEBUG1, (errmsg("hook_extend: %d %d %d %d",
		 rnode.backend, rnode.node.dbNode,
		 rnode.node.spcNode, rnode.node.relNode)));

	file_node_set(&rnode);
}

static void
hook_truncate(RelFileNodeBackend rnode)
{
	if (next_file_truncate_hook)
		next_file_truncate_hook(rnode);

	ereport(DEBUG1, (errmsg("hook_truncate: %d %d %d %d",
		 rnode.backend, rnode.node.dbNode,
		 rnode.node.spcNode, rnode.node.relNode)));

	file_node_set(&rnode);
}

static void
hook_unlink(RelFileNodeBackend rnode)
{
	if (next_file_unlink_hook)
		next_file_unlink_hook(rnode);

	ereport(DEBUG1, (errmsg("hook_unlink: %d %d %d %d",
		 rnode.backend, rnode.node.dbNode,
		 rnode.node.spcNode, rnode.node.relNode)));

	drops_track_add(rnode.node);
}

void
file_hook_init()
{
	next_file_create_hook = file_create_hook;
	file_create_hook = hook_create;

	next_file_extend_hook = file_extend_hook;
	file_extend_hook = hook_extend;

	next_file_truncate_hook = file_truncate_hook;
	file_truncate_hook = hook_truncate;

	next_file_unlink_hook = file_unlink_hook;
	file_unlink_hook = hook_unlink;
}

void
file_hook_deinit()
{
	file_create_hook = next_file_create_hook;
	file_extend_hook = next_file_extend_hook;
	file_truncate_hook = next_file_truncate_hook;
	file_unlink_hook = next_file_unlink_hook;
}
