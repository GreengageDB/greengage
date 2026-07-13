/*-------------------------------------------------------------------------
 *
 * walsender_private.h
 *	  Private definitions from replication/walsender.c.
 *
 * Portions Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *
 * src/include/replication/walsender_private.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALSENDER_PRIVATE_H
#define _WALSENDER_PRIVATE_H

#include "access/xlog.h"
#include "lib/ilist.h"
#include "nodes/nodes.h"
#include "nodes/replnodes.h"
#include "replication/syncrep.h"
#include "storage/condition_variable.h"
#include "storage/shmem.h"
#include "storage/spin.h"

typedef enum WalSndState
{
	WALSNDSTATE_STARTUP = 0,
	WALSNDSTATE_BACKUP,
	WALSNDSTATE_CATCHUP,
	WALSNDSTATE_STREAMING,
	WALSNDSTATE_STOPPING,
} WalSndState;

/*
 * Each walsender has a WalSnd struct in shared memory.
 *
 * This struct is protected by its 'mutex' spinlock field, except that some
 * members are only written by the walsender process itself, and thus that
 * process is free to read those members without holding spinlock.  pid and
 * needreload always require the spinlock to be held for all accesses.
 */
typedef struct WalSnd
{
	pid_t		pid;			/* this walsender's PID, or 0 if not active */

	WalSndState state;			/* this walsender's state */
	XLogRecPtr	sentPtr;		/* WAL has been sent up to this point */
	bool		sendKeepalive;	/* do we send keepalives on this connection? */
	bool		needreload;		/* does currently-open file need to be
								 * reloaded? */

	/*
	 * The xlog locations that have been written, flushed, and applied by
	 * standby-side. These may be invalid if the standby-side has not offered
	 * values yet.
	 */
	XLogRecPtr	write;
	XLogRecPtr	flush;
	XLogRecPtr	apply;

	/*
	 * This boolean indicates if this WAL sender has caught up within the
	 * range defined by user (guc). This helps the backends to decide if they
	 * should wait in the sync rep queue, should they see a live WAL sender
	 * but that is not yet in streaming state.
	 */
	bool		caughtup_within_range;

	/*
	 * xlog location up to which xlog seg file cleanup for this walsender
	 * is allowed.
	 * In case of backup mode, it is the starting xlog ptr and
	 * in case of actual xlog replication to a standby it is the
	 * either the write/flush xlog ptr
	 *
	 * Note:- Valid only when this walsender is alive
	 */
	XLogRecPtr	xlogCleanUpTo;

	/* Measured lag times, or -1 for unknown/none. */
	TimeOffset	writeLag;
	TimeOffset	flushLag;
	TimeOffset	applyLag;

	/*
	 * The priority order of the standby managed by this WALSender, as listed
	 * in synchronous_standby_names, or 0 if not-listed.
	 */
	int			sync_standby_priority;

	/* Protects shared variables in this structure. */
	slock_t		mutex;

	/*
	 * Timestamp of the last message received from standby.
	 */
	TimestampTz replyTime;

	/*
	 * Indicates whether the WalSnd represents a connection with a Greenplum
	 * mirror in streaming mode
	 */
	bool 		is_for_gp_walreceiver;

	ReplicationKind kind;
} WalSnd;

extern PGDLLIMPORT WalSnd *MyWalSnd;

typedef enum
{
	WALSNDERROR_NONE = 0,
	WALSNDERROR_WALREAD
} WalSndError;

/* There is one WalSndCtl struct for the whole database cluster */
typedef struct
{
	/*
	 * Synchronous replication queue with one queue per request type.
	 * Protected by SyncRepLock.
	 */
	dlist_head	SyncRepQueue[NUM_SYNC_REP_WAIT_MODE];

	/*
	 * Current location of the head of the queue. All waiters should have a
	 * waitLSN that follows this value. Protected by SyncRepLock.
	 */
	XLogRecPtr	lsn[NUM_SYNC_REP_WAIT_MODE];

	/*
	 * Status of data related to the synchronous standbys.  Waiting backends
	 * can't reload the config file safely, so checkpointer updates this value
	 * as needed. Protected by SyncRepLock.
	 */
	bits8		sync_standbys_status;

	/*
	 * xlog location up to which xlog seg file cleanup is allowed.
	 * Checkpoint creation cleans old non-required xlog files. We have to
	 * preserve old files in case where the backup dump is large and the
	 * old xlog seg files are not yet dumped out OR in case the walsender
	 * has just commenced but hasn't replicated all the old xlog seg file contents.
	 *
	 * This location is obtained by comparing 'xlogCleanUpTo'
	 * set by each active walsender.
	 *
	 * Note:- Valid only when atleast one walsender is alive
	 */
	XLogRecPtr	walsnd_xlogCleanUpTo;

	/*
	 * Indicate error state of WalSender, for example, missing XLOG for mirror
	 * to stream.
	 *
	 * Note: If we want to support multiple mirrors, this data structure
	 * need to be redesigned (e.g. using WalSndError[]). We cannot store this
	 * field in the walsnds[] array below, because the walsnds[] only
	 * tracks the live wal senders. Hence, if the wal sender goes away
	 * with certain error, the error state will go away with it.
	 *
	 */
	WalSndError error;

	/* used as a registry of physical / logical walsenders to wake */
	ConditionVariable wal_flush_cv;
	ConditionVariable wal_replay_cv;

	/*
	 * Used by physical walsenders holding slots specified in
	 * synchronized_standby_slots to wake up logical walsenders holding
	 * logical failover slots when a walreceiver confirms the receipt of LSN.
	 */
	ConditionVariable wal_confirm_rcv_cv;

	WalSnd		walsnds[FLEXIBLE_ARRAY_MEMBER];
} WalSndCtlData;

/* Flags for WalSndCtlData->sync_standbys_status */

/*
 * Is the synchronous standby data initialized from the GUC?  This is set the
 * first time synchronous_standby_names is processed by the checkpointer.
 */
#define SYNC_STANDBY_INIT			(1 << 0)

/*
 * Is the synchronous standby data defined?  This is set when
 * synchronous_standby_names has some data, after being processed by the
 * checkpointer.
 */
#define SYNC_STANDBY_DEFINED		(1 << 1)

extern PGDLLIMPORT WalSndCtlData *WalSndCtl;


extern void WalSndSetState(WalSndState state);

/*
 * Internal functions for parsing the replication grammar, in repl_gram.y and
 * repl_scanner.l
 */
union YYSTYPE;
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void *yyscan_t;
#endif
extern int	replication_yyparse(Node **replication_parse_result_p, yyscan_t yyscanner);
extern int	replication_yylex(union YYSTYPE *yylval_param, yyscan_t yyscanner);
pg_noreturn extern void replication_yyerror(Node **replication_parse_result_p, yyscan_t yyscanner, const char *message);
extern void replication_scanner_init(const char *str, yyscan_t *yyscannerp);
extern void replication_scanner_finish(yyscan_t yyscanner);
extern bool replication_scanner_is_replication_command(yyscan_t yyscanner);

#define GP_WALRECEIVER_APPNAME "gp_walreceiver"

#endif							/* _WALSENDER_PRIVATE_H */
