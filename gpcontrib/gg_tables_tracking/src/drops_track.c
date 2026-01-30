/*
 * Track unlink hook events.
 */

#include "drops_track.h"

#include "lib/ilist.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#include "gg_tables_tracking_guc.h"

/*
 * Drop track element. Stores just relfilenode
 * and dbid.
 */
typedef struct
{
	Oid			relNode;
	Oid			dbNode;
}	track_relfilenode_t;

/* Doubly linked list node of dropped file nodes */
typedef struct
{
	dlist_node	node;
	track_relfilenode_t relfileNode;
}	drops_track_node_t;

/* Drops track */
typedef struct
{
	dlist_head	used_head;
	dlist_head	free_head;
	uint32_t	used_count;		/* count of used nodes */
	char		nodes[FLEXIBLE_ARRAY_MEMBER];	/* array of drops_track_node_t */
}	drops_track_t;

static shmem_startup_hook_type next_shmem_startup_hook = NULL;
static drops_track_t *drops_track;
LWLock	   *drops_track_lock;

static inline drops_track_node_t *
track_node_get(drops_track_t *track, int i)
{
	return (drops_track_node_t *) (track->nodes + i * sizeof(drops_track_node_t));
}

static Size
drops_track_calc_size()
{
	Size		size;

	size = offsetof(drops_track_t, nodes);
	size = add_size(size, mul_size(drops_count, sizeof(drops_track_node_t)));

	return size;
}

static void
drops_track_hook(void)
{
	bool		found;
	Size		size = drops_track_calc_size();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	drops_track = ShmemInitStruct("adb_track_files_drops", size, &found);

	if (!found)
	{
		drops_track->used_count = 0;
		dlist_init(&drops_track->used_head);
		dlist_init(&drops_track->free_head);

		for (uint32_t i = 0; i < drops_count; i++)
		{
			drops_track_node_t *track_node = track_node_get(drops_track, i);

			track_node->relfileNode.relNode = InvalidOid;
			track_node->relfileNode.dbNode = InvalidOid;
			dlist_push_tail(&drops_track->free_head, &track_node->node);
		}
	}

	drops_track_lock = &(GetNamedLWLockTranche("gg_tables_tracking"))->lock;

	LWLockRelease(AddinShmemInitLock);

	if (next_shmem_startup_hook)
		next_shmem_startup_hook();
}

void
drops_track_init(void)
{
	RequestAddinShmemSpace(drops_track_calc_size());

	next_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = drops_track_hook;
}

void
drops_track_deinit(void)
{
	shmem_startup_hook = next_shmem_startup_hook;
}

static drops_track_node_t *
get_free_node(void)
{
	if (dlist_is_empty(&drops_track->free_head))
		return NULL;

	return (drops_track_node_t *) dlist_pop_head_node(&drops_track->free_head);
}

/* Add relNode to track. Old node is dropped if no space */
void
drops_track_add(RelFileNode relfileNode)
{
	drops_track_node_t *track_node;

	LWLockAcquire(drops_track_lock, LW_EXCLUSIVE);

	if (drops_track->used_count >= drops_count)
	{
		track_node = (drops_track_node_t *) dlist_pop_head_node(&drops_track->used_head);
		elog(DEBUG1, "No space for drop track. Oldest node removed (%d).", track_node->relfileNode.relNode);
	}
	else
	{
		track_node = get_free_node();
		drops_track->used_count++;
		Assert(track_node);
	}

	track_node->relfileNode.relNode = relfileNode.relNode;
	track_node->relfileNode.dbNode = relfileNode.dbNode;
	dlist_push_tail(&drops_track->used_head, &track_node->node);

	elog(DEBUG1, "added relNode %u for dbNode %u to drops track",
		 relfileNode.relNode, relfileNode.dbNode);

	LWLockRelease(drops_track_lock);
}

/* Extract relfilenodes corresponding to specific db into separeate list */
List *
drops_track_move(Oid dbid)
{
	List	   *oids = NIL;
	dlist_mutable_iter iter;

	LWLockAcquire(drops_track_lock, LW_EXCLUSIVE);

	if (drops_track->used_count == 0)
	{
		LWLockRelease(drops_track_lock);
		return NIL;
	}

	dlist_foreach_modify(iter, &drops_track->used_head)
	{
		drops_track_node_t *track_node = (drops_track_node_t *) iter.cur;

		if (track_node->relfileNode.dbNode == dbid)
		{
			oids = lcons_oid(track_node->relfileNode.relNode, oids);
			drops_track->used_count--;
			track_node->relfileNode.relNode = InvalidOid;
			track_node->relfileNode.dbNode = InvalidOid;
			dlist_delete(&track_node->node);
			dlist_push_tail(&drops_track->free_head, &track_node->node);
		}
	}

	LWLockRelease(drops_track_lock);

	return oids;
}
