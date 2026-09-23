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
	uint64		count;
} ProfileItem;

typedef struct
{
	int			pid;
	uint32		wait_event_info;
	uint64		queryId;
	int32		ssid;			/* GGDB session id */
	int32		ccnt;			/* GGDB command id */
	TimestampTz ts;
} HistoryItem;

typedef struct
{
	bool		wraparound;
	Size		index;
	Size		count;
	HistoryItem *items;
	bool		has_tmid;		/* declared result row has a tmid column */
} History;

/*
 * GGDB: session id and command id of a sampled process are read from its
 * PGPROC entry, so they are available whatever phase of a statement the
 * process is in, including waits before parsing. A process waiting for its
 * client is idle, or between messages of one command: it is attributed to
 * the session but to no command, so that an idle backend produces a single
 * profile entry rather than one per command it ever ran.
 */
static inline void
pgws_proc_identity(PGPROC *proc, uint32 wait_event_info,
				   int32 *ssid, int32 *ccnt)
{
	*ssid = proc->mppSessionId;
	*ccnt = (wait_event_info == WAIT_EVENT_CLIENT_READ) ? 0 : proc->queryCommandId;
}

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
extern shm_mq *pgws_collector_mq;
extern uint64	   *pgws_proc_queryids;
extern CollectorShmqHeader *pgws_collector_hdr;

extern pgwsLockSharedState *pgws_lss;

extern bool pgws_should_sample_proc(PGPROC *proc, int *pid_p, uint32 *wait_event_info_p);

/* collector.c */
extern void pgws_register_wait_collector(void);
extern PGDLLEXPORT void pgws_collector_main(Datum main_arg);

#endif
