/*
 * pg_wait_sampling.h
 *		Headers for pg_wait_sampling extension.
 *
 * Copyright (c) 2015-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gg_wait_sampling/pg_wait_sampling.h
 */
#ifndef __PG_WAIT_SAMPLING_H__
#define __PG_WAIT_SAMPLING_H__

#include "datatype/timestamp.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shm_mq.h"

#define	PG_WAIT_SAMPLING_MAGIC		0xCA94B107
#define COLLECTOR_QUEUE_SIZE		(16 * 1024)
#define HISTORY_TIME_MULTIPLIER		10
#define PGWS_QUEUE_LOCK_NAME		"pgws_queue_lock"
#define PGWS_COLLECTOR_LOCK_NAME	"pgws_collector_lock"

typedef struct
{
	int			pid;
	uint32		wait_event_info;
	uint64		queryId;
	int32		ssid;			/* GGDB session id */
	int32		ccnt;			/* GGDB command id */
	int32		tmid;			/* GGDB coordinator start time */
	uint64		count;
} ProfileItem;

typedef struct
{
	int			pid;
	uint32		wait_event_info;
	uint64		queryId;
	int32		ssid;			/* GGDB session id */
	int32		ccnt;			/* GGDB command id */
	int32		tmid;			/* GGDB coordinator start time */
	TimestampTz ts;
} HistoryItem;

typedef struct
{
	bool		wraparound;
	Size		index;
	Size		count;
	HistoryItem *items;
} History;

typedef enum
{
	NO_REQUEST,
	HISTORY_REQUEST,
	PROFILE_REQUEST,
	PROFILE_RESET
} SHMRequest;

typedef struct
{
	Latch	   *latch;
	SHMRequest	request;
	/*
	 * GGDB: postmaster start time of the coordinator that the sessions on
	 * this node belong to (tmid). Every session on a node belongs to the same
	 * coordinator, so one value per node is enough. See pgws_proc_identity().
	 */
	int32		cluster_tmid;
} CollectorShmqHeader;

/* LWLock pointers */
typedef struct pgwsLockSharedState
{
	LWLock	*queue_lock;
	LWLock	*collector_lock;
} pgwsLockSharedState;

/* GUC variables */
extern int	pgws_historySize;
extern int	pgws_historyPeriod;
extern int	pgws_profilePeriod;
extern bool pgws_profilePid;
extern int	pgws_profileQueries;
extern bool pgws_sampleCpu;

/* pg_wait_sampling.c */
extern CollectorShmqHeader *pgws_collector_hdr;
extern shm_mq *pgws_collector_mq;
extern uint64 *pgws_proc_queryids;
extern bool	  *pgws_proc_active;

extern pgwsLockSharedState *pgws_lss;

extern bool pgws_should_sample_proc(PGPROC *proc, int *pid_p, uint32 *wait_event_info_p);

/*
 * GGDB: session id and command id of a sampled process are read from its
 * PGPROC entry, so they are available whatever phase of a statement the
 * process is in, including waits before parsing. Processes without a session
 * report 0 rather than InvalidGpSessionId.
 *
 * The coordinator keeps queryCommandId after a statement ends, so a backend
 * waiting for its client outside a statement (idle, idle in transaction, or
 * between the messages of an extended-protocol command) would be attributed
 * to its previous command and every command would leave a profile entry. Such
 * a wait is attributed to no command instead. A client read inside a
 * statement, for example COPY FROM STDIN on the coordinator or a QE reading
 * the COPY data from the QD, keeps its command id: pgws_proc_active marks the
 * backends that are inside a utility statement.
 *
 * tmid is the coordinator's postmaster start time. A QE receives it in its
 * startup packet, so every backend of a session on any node knows it; each
 * backend publishes it once, from the first hook it runs, into the collector
 * header, and the collector seeds it on the coordinator node from its own
 * postmaster. Processes without a session (background and auxiliary
 * processes, utility-mode connections to a segment) report 0.
 */
static inline void
pgws_proc_identity(PGPROC *proc, HistoryItem *item)
{
	bool		idle = (item->wait_event_info == WAIT_EVENT_CLIENT_READ &&
						!pgws_proc_active[proc - ProcGlobal->allProcs]);

	item->ssid = (proc->mppSessionId > 0) ? proc->mppSessionId : 0;
	item->ccnt = idle ? 0 : proc->queryCommandId;
	item->tmid = (item->ssid > 0) ? pgws_collector_hdr->cluster_tmid : 0;
}

/* collector.c */
extern void pgws_register_wait_collector(void);
extern PGDLLEXPORT void pgws_collector_main(Datum main_arg);

#endif
