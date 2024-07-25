/*-------------------------------------------------------------------------
 *
 * proc.c
 *	  routines to manage per-process shared memory data structure
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/proc.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * Interface (a):
 *		ProcSleep(), ProcWakeup(),
 *		ProcQueueAlloc() -- create a shm queue for sleeping processes
 *		ProcQueueInit() -- create a queue without allocing memory
 *
 * Waiting for a lock causes the backend to be put to sleep.  Whoever releases
 * the lock wakes the process up again (and gives it an error code so it knows
 * whether it was awoken on an error condition).
 *
 * Interface (b):
 *
 * ProcReleaseLocks -- frees the locks associated with current transaction
 *
 * ProcKill -- destroys the shared memory state (and locks)
 * associated with the process.
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/time.h>

#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "catalog/namespace.h" /* TempNamespaceOidIsValid */
#include "commands/async.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "storage/condition_variable.h"
#include "storage/standby.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "storage/sinval.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/spin.h"
#include "utils/faultinjector.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

#include "utils/sharedsnapshot.h"  /*SharedLocalSnapshotSlot*/

#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"  /*Gp_is_writer*/
#include "port/atomics.h"
#include "postmaster/fts.h"
#include "utils/resource_manager.h"
#include "utils/resscheduler.h"
#include "utils/session_state.h"

/* GUC variables */
int			DeadlockTimeout = 1000;
int			StatementTimeout = 0;
int			LockTimeout = 0;
int			IdleInTransactionSessionTimeout = 0;
int			IdleSessionGangTimeout = 0;
bool		log_lock_waits = false;

/* Pointer to this process's PGPROC and PGXACT structs, if any */
PGPROC	   *MyProc = NULL;
PGXACT	   *MyPgXact = NULL;
TMGXACT	   *MyTmGxact = NULL;
TMGXACTLOCAL	*MyTmGxactLocal = NULL;

/* Special for MPP reader gangs */
PGPROC	   *lockHolderProcPtr;

/*
 * This spinlock protects the freelist of recycled PGPROC structures.
 * We cannot use an LWLock because the LWLock manager depends on already
 * having a PGPROC and a wait semaphore!  But these structures are touched
 * relatively infrequently (only at backend startup or shutdown) and not for
 * very long, so a spinlock is okay.
 */
NON_EXEC_STATIC slock_t *ProcStructLock = NULL;

/* Pointers to shared-memory structures */
PROC_HDR   *ProcGlobal = NULL;
NON_EXEC_STATIC PGPROC *AuxiliaryProcs = NULL;
PGPROC	   *PreparedXactProcs = NULL;

/* If we are waiting for a lock, this points to the associated LOCALLOCK */
static LOCALLOCK *lockAwaited = NULL;

static DeadLockState deadlock_state = DS_NOT_YET_CHECKED;

/* Is a deadlock check pending? */
static volatile sig_atomic_t got_deadlock_timeout;

static void RemoveProcFromArray(int code, Datum arg);
static void ProcKill(int code, Datum arg);
static void AuxiliaryProcKill(int code, Datum arg);
static void CheckDeadLock(void);


/*
 * Report shared-memory space needed by InitProcGlobal.
 */
Size
ProcGlobalShmemSize(void)
{
	Size		size = 0;

	/* ProcGlobal */
	size = add_size(size, sizeof(PROC_HDR));
	/* MyProcs, including autovacuum workers and launcher */
	size = add_size(size, mul_size(MaxBackends, sizeof(PGPROC)));
	/* AuxiliaryProcs */
	size = add_size(size, mul_size(NUM_AUXILIARY_PROCS, sizeof(PGPROC)));
	/* Prepared xacts */
	size = add_size(size, mul_size(max_prepared_xacts, sizeof(PGPROC)));
	/* ProcStructLock */
	size = add_size(size, sizeof(slock_t));

	size = add_size(size, mul_size(MaxBackends, sizeof(PGXACT)));
	size = add_size(size, mul_size(NUM_AUXILIARY_PROCS, sizeof(PGXACT)));
	size = add_size(size, mul_size(max_prepared_xacts, sizeof(PGXACT)));

	return size;
}

/*
 * Report number of semaphores needed by InitProcGlobal.
 */
int
ProcGlobalSemas(void)
{
	/*
	 * We need a sema per backend (including autovacuum), plus one for each
	 * auxiliary process.
	 */
	return MaxBackends + NUM_AUXILIARY_PROCS;
}

/*
 * InitProcGlobal -
 *	  Initialize the global process table during postmaster or standalone
 *	  backend startup.
 *
 *	  We also create all the per-process semaphores we will need to support
 *	  the requested number of backends.  We used to allocate semaphores
 *	  only when backends were actually started up, but that is bad because
 *	  it lets Postgres fail under load --- a lot of Unix systems are
 *	  (mis)configured with small limits on the number of semaphores, and
 *	  running out when trying to start another backend is a common failure.
 *	  So, now we grab enough semaphores to support the desired max number
 *	  of backends immediately at initialization --- if the sysadmin has set
 *	  MaxConnections, max_worker_processes, max_wal_senders, or
 *	  autovacuum_max_workers higher than his kernel will support, he'll
 *	  find out sooner rather than later.
 *
 *	  Another reason for creating semaphores here is that the semaphore
 *	  implementation typically requires us to create semaphores in the
 *	  postmaster, not in backends.
 *
 * Note: this is NOT called by individual backends under a postmaster,
 * not even in the EXEC_BACKEND case.  The ProcGlobal and AuxiliaryProcs
 * pointers must be propagated specially for EXEC_BACKEND operation.
 */
void
InitProcGlobal(void)
{
	PGPROC	   *procs;
	PGXACT	   *pgxacts;
	TMGXACT	   *tmgxacts;
	int			i,
				j;
	bool		found;
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;

	/* Create the ProcGlobal shared structure */
	ProcGlobal = (PROC_HDR *)
		ShmemInitStruct("Proc Header", sizeof(PROC_HDR), &found);
	Assert(!found);

	/*
	 * Initialize the data structures.
	 */
	ProcGlobal->spins_per_delay = DEFAULT_SPINS_PER_DELAY;
	ProcGlobal->freeProcs = NULL;
	ProcGlobal->autovacFreeProcs = NULL;
	ProcGlobal->bgworkerFreeProcs = NULL;
	ProcGlobal->walsenderFreeProcs = NULL;
	ProcGlobal->startupProc = NULL;
	ProcGlobal->startupProcPid = 0;
	ProcGlobal->startupBufferPinWaitBufId = -1;
	ProcGlobal->walwriterLatch = NULL;
	ProcGlobal->checkpointerLatch = NULL;
	pg_atomic_init_u32(&ProcGlobal->procArrayGroupFirst, INVALID_PGPROCNO);
	pg_atomic_init_u32(&ProcGlobal->clogGroupFirst, INVALID_PGPROCNO);

	ProcGlobal->mppLocalProcessCounter = 0;

	/*
	 * Create and initialize all the PGPROC structures we'll need.  There are
	 * five separate consumers: (1) normal backends, (2) autovacuum workers
	 * and the autovacuum launcher, (3) background workers, (4) auxiliary
	 * processes, and (5) prepared transactions.  Each PGPROC structure is
	 * dedicated to exactly one of these purposes, and they do not move
	 * between groups.
	 */
	procs = (PGPROC *) ShmemAlloc(TotalProcs * sizeof(PGPROC));
	MemSet(procs, 0, TotalProcs * sizeof(PGPROC));
	ProcGlobal->allProcs = procs;
	/* XXX allProcCount isn't really all of them; it excludes prepared xacts */
	ProcGlobal->allProcCount = MaxBackends + NUM_AUXILIARY_PROCS;

	/*
	 * Also allocate a separate array of PGXACT structures.  This is separate
	 * from the main PGPROC array so that the most heavily accessed data is
	 * stored contiguously in memory in as few cache lines as possible. This
	 * provides significant performance benefits, especially on a
	 * multiprocessor system.  There is one PGXACT structure for every PGPROC
	 * structure.
	 */
	pgxacts = (PGXACT *) ShmemAlloc(TotalProcs * sizeof(PGXACT));
	MemSet(pgxacts, 0, TotalProcs * sizeof(PGXACT));
	ProcGlobal->allPgXact = pgxacts;

	/*
	 * Also allocate a separate array of TMGXACT structures out of the same
	 * consideration as above.
	 */
	tmgxacts = (TMGXACT *) ShmemAlloc(TotalProcs * sizeof(TMGXACT));
	MemSet(tmgxacts, 0, TotalProcs * sizeof(TMGXACT));
	ProcGlobal->allTmGxact = tmgxacts;

	for (i = 0; i < TotalProcs; i++)
	{
		/* Common initialization for all PGPROCs, regardless of type. */

		/*
		 * Set up per-PGPROC semaphore, latch, and backendLock. Prepared xact
		 * dummy PGPROCs don't need these though - they're never associated
		 * with a real process
		 */
		if (i < MaxBackends + NUM_AUXILIARY_PROCS)
		{
			procs[i].sem = PGSemaphoreCreate();
			InitSharedLatch(&(procs[i].procLatch));
			LWLockInitialize(&(procs[i].backendLock), LWTRANCHE_PROC);
		}
		procs[i].pgprocno = i;

		/*
		 * Newly created PGPROCs for normal backends, autovacuum and bgworkers
		 * must be queued up on the appropriate free list.  Because there can
		 * only ever be a small, fixed number of auxiliary processes, no free
		 * list is used in that case; InitAuxiliaryProcess() instead uses a
		 * linear search.   PGPROCs for prepared transactions are added to a
		 * free list by TwoPhaseShmemInit().
		 */
		if (i < MaxConnections)
		{
			/* PGPROC for normal backend, add to freeProcs list */
			procs[i].links.next = (SHM_QUEUE *) ProcGlobal->freeProcs;
			ProcGlobal->freeProcs = &procs[i];
			procs[i].procgloballist = &ProcGlobal->freeProcs;
		}
		else if (i < MaxConnections + autovacuum_max_workers + 1)
		{
			/* PGPROC for AV launcher/worker, add to autovacFreeProcs list */
			procs[i].links.next = (SHM_QUEUE *) ProcGlobal->autovacFreeProcs;
			ProcGlobal->autovacFreeProcs = &procs[i];
			procs[i].procgloballist = &ProcGlobal->autovacFreeProcs;
		}
		else if (i < MaxConnections + autovacuum_max_workers + 1 + max_worker_processes)
		{
			/* PGPROC for bgworker, add to bgworkerFreeProcs list */
			procs[i].links.next = (SHM_QUEUE *) ProcGlobal->bgworkerFreeProcs;
			ProcGlobal->bgworkerFreeProcs = &procs[i];
			procs[i].procgloballist = &ProcGlobal->bgworkerFreeProcs;
		}
		else if (i < MaxBackends)
		{
			/* PGPROC for walsender, add to walsenderFreeProcs list */
			procs[i].links.next = (SHM_QUEUE *) ProcGlobal->walsenderFreeProcs;
			ProcGlobal->walsenderFreeProcs = &procs[i];
			procs[i].procgloballist = &ProcGlobal->walsenderFreeProcs;
		}

		/* Initialize myProcLocks[] shared memory queues. */
		for (j = 0; j < NUM_LOCK_PARTITIONS; j++)
			SHMQueueInit(&(procs[i].myProcLocks[j]));

		/* Initialize lockGroupMembers list. */
		dlist_init(&procs[i].lockGroupMembers);

		/*
		 * Initialize the atomic variables, otherwise, it won't be safe to
		 * access them for backends that aren't currently in use.
		 */
		pg_atomic_init_u32(&(procs[i].procArrayGroupNext), INVALID_PGPROCNO);
		pg_atomic_init_u32(&(procs[i].clogGroupNext), INVALID_PGPROCNO);
	}

	/*
	 * Save pointers to the blocks of PGPROC structures reserved for auxiliary
	 * processes and prepared transactions.
	 */
	AuxiliaryProcs = &procs[MaxBackends];
	PreparedXactProcs = &procs[MaxBackends + NUM_AUXILIARY_PROCS];

	/* Create ProcStructLock spinlock, too */
	ProcStructLock = (slock_t *) ShmemAlloc(sizeof(slock_t));
	SpinLockInit(ProcStructLock);
}

/*
 * InitProcess -- initialize a per-process data structure for this backend
 */
void
InitProcess(void)
{
	PGPROC	   *volatile *procgloballist;

	/*
	 * WAL sender, etc are marked as GP_ROLE_UTILITY to prevent unwanted
	 * GP_ROLE_DISPATCH MyProc settings such as mppSessionId being valid and
	 * mppIsWriter set to true.
	 */
	if (am_walsender || am_ftshandler || am_faulthandler)
		Gp_role = GP_ROLE_UTILITY;

	/*
	 * ProcGlobal should be set up already (if we are a backend, we inherit
	 * this by fork() or EXEC_BACKEND mechanism from the postmaster).
	 */
	if (ProcGlobal == NULL)
		elog(PANIC, "proc header uninitialized");

	if (MyProc != NULL)
		elog(ERROR, "you already exist");

	/* Decide which list should supply our PGPROC. */
	if (IsAnyAutoVacuumProcess())
		procgloballist = &ProcGlobal->autovacFreeProcs;
	else if (IsBackgroundWorker)
		procgloballist = &ProcGlobal->bgworkerFreeProcs;
	else if (am_walsender)
		procgloballist = &ProcGlobal->walsenderFreeProcs;
	else
		procgloballist = &ProcGlobal->freeProcs;

	/*
	 * Try to get a proc struct from the appropriate free list.  If this
	 * fails, we must be out of PGPROC structures (not to mention semaphores).
	 *
	 * While we are holding the ProcStructLock, also copy the current shared
	 * estimate of spins_per_delay to local storage.
	 */
	SpinLockAcquire(ProcStructLock);

	set_spins_per_delay(ProcGlobal->spins_per_delay);

	MyProc = *procgloballist;

	if (MyProc != NULL)
	{
		*procgloballist = (PGPROC *) MyProc->links.next;
		SpinLockRelease(ProcStructLock);
	}
	else
	{
		/*
		 * If we reach here, all the PGPROCs are in use.  This is one of the
		 * possible places to detect "too many backends", so give the standard
		 * error message.  XXX do we need to give a different failure message
		 * in the autovacuum case?
		 */
		SpinLockRelease(ProcStructLock);
		if (am_walsender)
			ereport(FATAL,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
					 errmsg("number of requested standby connections exceeds max_wal_senders (currently %d)",
							max_wal_senders)));
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("sorry, too many clients already")));
	}
	MyPgXact = &ProcGlobal->allPgXact[MyProc->pgprocno];
	MyTmGxact = &ProcGlobal->allTmGxact[MyProc->pgprocno];
	MyTmGxactLocal = (TMGXACTLOCAL*)MemoryContextAllocZero(TopMemoryContext, sizeof(TMGXACTLOCAL));
	if (MyTmGxactLocal == NULL)
		elog(FATAL, "allocating TMGXACTLOCAL failed");

	if (gp_debug_pgproc)
	{
		elog(LOG, "allocating PGPROC entry for pid %d, freeProcs (prev ptr, new ptr): (%p, %p)",
			 MyProcPid, MyProc, MyProc->links.next);
	}

	int mppLocalProcessSerial = pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&ProcGlobal->mppLocalProcessCounter, 1);

	lockHolderProcPtr = MyProc;

	/* Set the next pointer to NULL */
	MyProc->links.next = NULL;

	/*
	 * Cross-check that the PGPROC is of the type we expect; if this were not
	 * the case, it would get returned to the wrong list.
	 */
	Assert(MyProc->procgloballist == procgloballist);

	/*
	 * Now that we have a PGPROC, mark ourselves as an active postmaster
	 * child; this is so that the postmaster can detect it if we exit without
	 * cleaning up.  (XXX autovac launcher currently doesn't participate in
	 * this; it probably should.)
	 *
	 * Ideally, we should create functions similar to IsAutoVacuumLauncherProcess()
	 * for ftsProber, etc who call InitProcess().
	 * But MyPMChildSlot helps to get away with it.
	 */
	if (IsUnderPostmaster && !IsAutoVacuumLauncherProcess()
		&& MyPMChildSlot > 0)
		MarkPostmasterChildActive();

	/*
	 * Initialize all fields of MyProc, except for those previously
	 * initialized by InitProcGlobal.
	 */
	SHMQueueElemInit(&(MyProc->links));
	MyProc->waitStatus = STATUS_OK;
	MyProc->lxid = InvalidLocalTransactionId;
	MyProc->fpVXIDLock = false;
	MyProc->fpLocalTransactionId = InvalidLocalTransactionId;
	MyPgXact->xid = InvalidTransactionId;
	MyPgXact->xmin = InvalidTransactionId;
	MyProc->localDistribXactData.state = LOCALDISTRIBXACT_STATE_NONE;
	MyProc->pid = MyProcPid;
	/* backendId, databaseId and roleId will be filled in later */
	MyProc->backendId = InvalidBackendId;
	MyProc->databaseId = InvalidOid;
	MyProc->roleId = InvalidOid;
	MyProc->tempNamespaceId = InvalidOid;
	MyProc->isBackgroundWorker = IsBackgroundWorker;
	MyPgXact->delayChkpt = 0;
	MyPgXact->vacuumFlags = 0;
	/* NB -- autovac launcher intentionally does not set IS_AUTOVACUUM */
	if (IsAutoVacuumWorkerProcess())
		MyPgXact->vacuumFlags |= PROC_IS_AUTOVACUUM;
	MyProc->lwWaiting = false;
	MyProc->lwWaitMode = 0;
	MyProc->waitLock = NULL;
	MyProc->waitProcLock = NULL;
	MyProc->resSlot = NULL;
	SpinLockInit(&MyProc->movetoMutex);
	MyProc->movetoResSlot = NULL;
	MyProc->movetoGroupId = InvalidOid;
	MyProc->movetoCallerPid = InvalidPid;

    /* 
     * mppLocalProcessSerial uniquely identifies this backend process among
     * all those that our parent postmaster process creates over its lifetime. 
     *
  	 * Since we use the process serial number to decide if we should
	 * deliver a response from a server under this spin, we need to 
	 * assign it under the spin lock.
	 */
    MyProc->mppLocalProcessSerial = mppLocalProcessSerial;

    /* 
     * A nonzero gp_session_id uniquely identifies an MPP client session 
     * over the lifetime of the entry postmaster process. A qDisp passes
     * its gp_session_id down to all of its qExecs. If this is a qExec,
     * we have already received the gp_session_id from the qDisp.
	 *
	 * Utility mode connections on segments should not be assigned a valid
	 * session ID.  Otherwise, locks acquired by them may result in incorrect
	 * determination of conflicts.  See LockCheckConflicts().
	 *
	 * It is ok to assign a valid session ID to a utility mode connection on
	 * coordinator, because session IDs are generated only on coordinator by atomically
	 * incrementing a counter.  Therefore, it is not possible for a utility
	 * mode connection to be assigned the same session ID as a normal mode
	 * connection on coordinator.
     */
	if (IS_QUERY_DISPATCHER() &&
		Gp_role == GP_ROLE_DISPATCH &&
		gp_session_id == InvalidGpSessionId)
        gp_session_id = mppLocalProcessSerial;

	AssertImply(Gp_role == GP_ROLE_UTILITY && !IS_QUERY_DISPATCHER(),
				gp_session_id == InvalidGpSessionId);

    MyProc->mppSessionId = gp_session_id;
    elog(DEBUG1,"InitProcess(): gp_session_id %d, Gp_role %d",gp_session_id, Gp_role);
    
    MyProc->mppIsWriter = Gp_is_writer;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		MyProc->mppIsWriter = true;
	}
    
	/* Initialise for sync rep */
#ifdef USE_ASSERT_CHECKING
	{
		int			i;

		/* Last process should have released all locks. */
		for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
			Assert(SHMQueueEmpty(&(MyProc->myProcLocks[i])));
	}
#endif
	MyProc->recoveryConflictPending = false;

	/* Initialize fields for sync rep */
	MyProc->waitLSN = 0;
	MyProc->syncRepState = SYNC_REP_NOT_WAITING;
	SHMQueueElemInit(&(MyProc->syncRepLinks));

	/* Initialize fields for group XID clearing. */
	MyProc->procArrayGroupMember = false;
	MyProc->procArrayGroupMemberXid = InvalidTransactionId;
	Assert(pg_atomic_read_u32(&MyProc->procArrayGroupNext) == INVALID_PGPROCNO);

	/* Check that group locking fields are in a proper initial state. */
	Assert(MyProc->lockGroupLeader == NULL);
	Assert(dlist_is_empty(&MyProc->lockGroupMembers));

	/* Initialize wait event information. */
	MyProc->wait_event_info = 0;

	/* Initialize fields for group transaction status update. */
	MyProc->clogGroupMember = false;
	MyProc->clogGroupMemberXid = InvalidTransactionId;
	MyProc->clogGroupMemberXidStatus = TRANSACTION_STATUS_IN_PROGRESS;
	MyProc->clogGroupMemberPage = -1;
	MyProc->clogGroupMemberLsn = InvalidXLogRecPtr;
	Assert(pg_atomic_read_u32(&MyProc->clogGroupNext) == INVALID_PGPROCNO);

	/*
	 * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch
	 * on it.  That allows us to repoint the process latch, which so far
	 * points to process local one, to the shared one.
	 */
	OwnLatch(&MyProc->procLatch);
	SwitchToSharedLatch();

	/*
	 * We might be reusing a semaphore that belonged to a failed process. So
	 * be careful and reinitialize its value here.  (This is not strictly
	 * necessary anymore, but seems like a good idea for cleanliness.)
	 */
	PGSemaphoreReset(MyProc->sem);

	/* Set wait portal (do not check if resource scheduling is enabled) */
	MyProc->waitPortalId = INVALID_PORTALID;

	MyProc->queryCommandId = -1;

	/* Init gxact */
	MyTmGxact->gxid = InvalidDistributedTransactionId;
	resetTmGxact();

	/*
	 * Arrange to clean up at backend exit.
	 */
	on_shmem_exit(ProcKill, 0);

	/*
	 * Now that we have a PGPROC, we could try to acquire locks, so initialize
	 * local state needed for LWLocks, and the deadlock checker.
	 */
	InitLWLockAccess();
	InitDeadLockChecking();
}

/*
 * InitProcessPhase2 -- make MyProc visible in the shared ProcArray.
 *
 * This is separate from InitProcess because we can't acquire LWLocks until
 * we've created a PGPROC, but in the EXEC_BACKEND case ProcArrayAdd won't
 * work until after we've done CreateSharedMemoryAndSemaphores.
 */
void
InitProcessPhase2(void)
{
	Assert(MyProc != NULL);

	/*
	 * Add our PGPROC to the PGPROC array in shared memory.
	 */
	ProcArrayAdd(MyProc);

	/*
	 * Arrange to clean that up at backend exit.
	 */
	on_shmem_exit(RemoveProcFromArray, 0);
}

/*
 * InitAuxiliaryProcess -- create a per-auxiliary-process data structure
 *
 * This is called by bgwriter and similar processes so that they will have a
 * MyProc value that's real enough to let them wait for LWLocks.  The PGPROC
 * and sema that are assigned are one of the extra ones created during
 * InitProcGlobal.
 *
 * Auxiliary processes are presently not expected to wait for real (lockmgr)
 * locks, so we need not set up the deadlock checker.  They are never added
 * to the ProcArray or the sinval messaging mechanism, either.  They also
 * don't get a VXID assigned, since this is only useful when we actually
 * hold lockmgr locks.
 *
 * Startup process however uses locks but never waits for them in the
 * normal backend sense. Startup process also takes part in sinval messaging
 * as a sendOnly process, so never reads messages from sinval queue. So
 * Startup process does have a VXID and does show up in pg_locks.
 */
void
InitAuxiliaryProcess(void)
{
	PGPROC	   *auxproc;
	int			proctype;

	/*
	 * ProcGlobal should be set up already (if we are a backend, we inherit
	 * this by fork() or EXEC_BACKEND mechanism from the postmaster).
	 */
	if (ProcGlobal == NULL || AuxiliaryProcs == NULL)
		elog(PANIC, "proc header uninitialized");

	if (MyProc != NULL)
		elog(ERROR, "you already exist");

	/*
	 * We use the ProcStructLock to protect assignment and releasing of
	 * AuxiliaryProcs entries.
	 *
	 * While we are holding the ProcStructLock, also copy the current shared
	 * estimate of spins_per_delay to local storage.
	 */
	SpinLockAcquire(ProcStructLock);

	set_spins_per_delay(ProcGlobal->spins_per_delay);

	/*
	 * Find a free auxproc ... *big* trouble if there isn't one ...
	 */
	for (proctype = 0; proctype < NUM_AUXILIARY_PROCS; proctype++)
	{
		auxproc = &AuxiliaryProcs[proctype];
		if (auxproc->pid == 0)
			break;
	}
	if (proctype >= NUM_AUXILIARY_PROCS)
	{
		SpinLockRelease(ProcStructLock);
		elog(FATAL, "all AuxiliaryProcs are in use");
	}

	/* Mark auxiliary proc as in use by me */
	/* use volatile pointer to prevent code rearrangement */
	((volatile PGPROC *) auxproc)->pid = MyProcPid;

	MyProc = auxproc;
	lockHolderProcPtr = auxproc;
	MyPgXact = &ProcGlobal->allPgXact[auxproc->pgprocno];
	MyTmGxact = &ProcGlobal->allTmGxact[auxproc->pgprocno];
	MyTmGxactLocal = (TMGXACTLOCAL*)MemoryContextAllocZero(TopMemoryContext, sizeof(TMGXACTLOCAL));
	if (MyTmGxactLocal == NULL)
		elog(FATAL, "allocating TMGXACTLOCAL failed");

	SpinLockRelease(ProcStructLock);

	/*
	 * Initialize all fields of MyProc, except for those previously
	 * initialized by InitProcGlobal.
	 */
	SHMQueueElemInit(&(MyProc->links));
	MyProc->waitStatus = STATUS_OK;
	MyProc->lxid = InvalidLocalTransactionId;
	MyProc->fpVXIDLock = false;
	MyProc->fpLocalTransactionId = InvalidLocalTransactionId;
	MyPgXact->xid = InvalidTransactionId;
	MyPgXact->xmin = InvalidTransactionId;
	MyProc->localDistribXactData.state = LOCALDISTRIBXACT_STATE_NONE;
	MyProc->backendId = InvalidBackendId;
	MyProc->databaseId = InvalidOid;
	MyProc->roleId = InvalidOid;
    MyProc->mppLocalProcessSerial = 0;
	MyProc->mppSessionId = InvalidGpSessionId;
    MyProc->mppIsWriter = false;
	MyProc->tempNamespaceId = InvalidOid;
	MyProc->isBackgroundWorker = IsBackgroundWorker;
	MyPgXact->delayChkpt = 0;
	MyPgXact->vacuumFlags = 0;
	MyProc->lwWaiting = false;
	MyProc->lwWaitMode = 0;
	MyProc->waitLock = NULL;
	MyProc->waitProcLock = NULL;
#ifdef USE_ASSERT_CHECKING
	{
		int			i;

		/* Last process should have released all locks. */
		for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
			Assert(SHMQueueEmpty(&(MyProc->myProcLocks[i])));
	}
#endif

	/*
	 * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch
	 * on it.  That allows us to repoint the process latch, which so far
	 * points to process local one, to the shared one.
	 */
	OwnLatch(&MyProc->procLatch);
	SwitchToSharedLatch();

	/* Check that group locking fields are in a proper initial state. */
	Assert(MyProc->lockGroupLeader == NULL);
	Assert(dlist_is_empty(&MyProc->lockGroupMembers));

	/*
	 * We might be reusing a semaphore that belonged to a failed process. So
	 * be careful and reinitialize its value here.  (This is not strictly
	 * necessary anymore, but seems like a good idea for cleanliness.)
	 */
	PGSemaphoreReset(MyProc->sem);

	MyProc->queryCommandId = -1;

	/*
	 * Arrange to clean up at process exit.
	 */
	on_shmem_exit(AuxiliaryProcKill, Int32GetDatum(proctype));
}

/*
 * Record the PID and PGPROC structures for the Startup process, for use in
 * ProcSendSignal().  See comments there for further explanation.
 */
void
PublishStartupProcessInformation(void)
{
	SpinLockAcquire(ProcStructLock);

	ProcGlobal->startupProc = MyProc;
	ProcGlobal->startupProcPid = MyProcPid;

	SpinLockRelease(ProcStructLock);
}

/*
 * Used from bufgr to share the value of the buffer that Startup waits on,
 * or to reset the value to "not waiting" (-1). This allows processing
 * of recovery conflicts for buffer pins. Set is made before backends look
 * at this value, so locking not required, especially since the set is
 * an atomic integer set operation.
 */
void
SetStartupBufferPinWaitBufId(int bufid)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile PROC_HDR *procglobal = ProcGlobal;

	procglobal->startupBufferPinWaitBufId = bufid;
}

/*
 * Used by backends when they receive a request to check for buffer pin waits.
 */
int
GetStartupBufferPinWaitBufId(void)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile PROC_HDR *procglobal = ProcGlobal;

	return procglobal->startupBufferPinWaitBufId;
}

/*
 * Check whether there are at least N free PGPROC objects.
 *
 * Note: this is designed on the assumption that N will generally be small.
 */
bool
HaveNFreeProcs(int n)
{
	PGPROC	   *proc;

	SpinLockAcquire(ProcStructLock);

	proc = ProcGlobal->freeProcs;

	while (n > 0 && proc != NULL)
	{
		proc = (PGPROC *) proc->links.next;
		n--;
	}

	SpinLockRelease(ProcStructLock);

	return (n <= 0);
}

/*
 * Check if the current process is awaiting a lock.
 */
bool
IsWaitingForLock(void)
{
	if (lockAwaited == NULL)
		return false;

	return true;
}

/*
 * Cancel any pending wait for lock, when aborting a transaction, and revert
 * any strong lock count acquisition for a lock being acquired.
 *
 * (Normally, this would only happen if we accept a cancel/die
 * interrupt while waiting; but an ereport(ERROR) before or during the lock
 * wait is within the realm of possibility, too.)
 */
void
LockErrorCleanup(void)
{
	LWLock	   *partitionLock;
	DisableTimeoutParams timeouts[2];

	HOLD_INTERRUPTS();

	AbortStrongLockAcquire();

	/* Nothing to do if we weren't waiting for a lock */
	if (lockAwaited == NULL)
	{
		RESUME_INTERRUPTS();
		return;
	}

	/* Don't try to cancel resource locks.*/
	if (Gp_role == GP_ROLE_DISPATCH && IsResQueueEnabled() &&
		LOCALLOCK_LOCKMETHOD(*lockAwaited) == RESOURCE_LOCKMETHOD)
	{
		RESUME_INTERRUPTS();
		return;
	}

	/*
	 * Turn off the deadlock and lock timeout timers, if they are still
	 * running (see ProcSleep).  Note we must preserve the LOCK_TIMEOUT
	 * indicator flag, since this function is executed before
	 * ProcessInterrupts when responding to SIGINT; else we'd lose the
	 * knowledge that the SIGINT came from a lock timeout and not an external
	 * source.
	 */
	timeouts[0].id = DEADLOCK_TIMEOUT;
	timeouts[0].keep_indicator = false;
	timeouts[1].id = LOCK_TIMEOUT;
	timeouts[1].keep_indicator = true;
	disable_timeouts(timeouts, 2);

	/* Unlink myself from the wait queue, if on it (might not be anymore!) */
	partitionLock = LockHashPartitionLock(lockAwaited->hashcode);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	if (MyProc->links.next != NULL)
	{
		/* We could not have been granted the lock yet */
		RemoveFromWaitQueue(MyProc, lockAwaited->hashcode);
	}
	else
	{
		/*
		 * Somebody kicked us off the lock queue already.  Perhaps they
		 * granted us the lock, or perhaps they detected a deadlock. If they
		 * did grant us the lock, we'd better remember it in our local lock
		 * table.
		 */
		if (MyProc->waitStatus == STATUS_OK)
			GrantAwaitedLock();
	}

	lockAwaited = NULL;

	LWLockRelease(partitionLock);

	RESUME_INTERRUPTS();
}


/*
 * ProcReleaseLocks() -- release locks associated with current transaction
 *			at main transaction commit or abort
 *
 * At main transaction commit, we release standard locks except session locks.
 * At main transaction abort, we release all locks including session locks.
 *
 * Advisory locks are released only if they are transaction-level;
 * session-level holds remain, whether this is a commit or not.
 *
 * At subtransaction commit, we don't release any locks (so this func is not
 * needed at all); we will defer the releasing to the parent transaction.
 * At subtransaction abort, we release all locks held by the subtransaction;
 * this is implemented by retail releasing of the locks under control of
 * the ResourceOwner mechanism.
 */
void
ProcReleaseLocks(bool isCommit)
{
	if (!MyProc)
		return;
	/* If waiting, get off wait queue (should only be needed after error) */
	LockErrorCleanup();
	/* Release standard locks, including session-level if aborting */
	LockReleaseAll(DEFAULT_LOCKMETHOD, !isCommit);
	/* Release transaction-level advisory locks */
	LockReleaseAll(USER_LOCKMETHOD, false);
}


/*
 * RemoveProcFromArray() -- Remove this process from the shared ProcArray.
 */
static void
RemoveProcFromArray(int code, Datum arg)
{
	Assert(MyProc != NULL);
	ProcArrayRemove(MyProc, InvalidTransactionId);
}

/*
 * update_spins_per_delay
 *   Update spins_per_delay value in ProcGlobal.
 */
static void
update_spins_per_delay(void)
{
	volatile PROC_HDR *procglobal = ProcGlobal;
	bool casResult = false;

	while (!casResult)
	{
		int old_spins_per_delay = procglobal->spins_per_delay;
		int new_spins_per_delay = recompute_spins_per_delay(old_spins_per_delay);
		casResult = pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)&procglobal->spins_per_delay,
										(uint32 *)&old_spins_per_delay,
										new_spins_per_delay);
	}
}

/*
 * ProcKill() -- Destroy the per-proc data structure for
 *		this process. Release any of its held LW locks.
 */
static void
ProcKill(int code, Datum arg)
{
	PGPROC	   *proc;
	PGPROC	   *volatile *procgloballist;

	Assert(MyProc != NULL);

	SIMPLE_FAULT_INJECTOR("proc_kill");
	/* not safe if forked by system(), etc. */
	if (MyProc->pid != (int) getpid())
		elog(PANIC, "ProcKill() called in child process");

	/* Make sure we're out of the sync rep lists */
	SyncRepCleanupAtProcExit();

	/* 
	 * Cleanup for any resource locks on portals - from holdable cursors or
	 * unclean process abort (assertion failures).
	 */
	if (Gp_role == GP_ROLE_DISPATCH && IsResQueueEnabled())
		AtExitCleanup_ResPortals();

	/*
	 * Remove the shared snapshot slot.
	 */
	if (SharedLocalSnapshotSlot != NULL)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			SharedSnapshotRemove(SharedLocalSnapshotSlot,
								 "Query Dispatcher");
		}
	    else if (IS_QUERY_DISPATCHER() && Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer)
	    {
			/* 
			 * Entry db singleton QE is a user of the shared snapshot -- not a creator.
			 */	
	    }
		else if (Gp_role == GP_ROLE_EXECUTE && Gp_is_writer)
		{
			SharedSnapshotRemove(SharedLocalSnapshotSlot,
								 "Writer qExec");
		}
		SharedLocalSnapshotSlot = NULL;
	}

	SyncRepCleanupAtProcExit();

#ifdef USE_ASSERT_CHECKING
	{
		int			i;

		/* Last process should have released all locks. */
		for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
			Assert(SHMQueueEmpty(&(MyProc->myProcLocks[i])));
	}
#endif

	/*
	 * Release any LW locks I am holding.  There really shouldn't be any, but
	 * it's cheap to check again before we cut the knees off the LWLock
	 * facility by releasing our PGPROC ...
	 */
	LWLockReleaseAll();

	/* Cancel any pending condition variable sleep, too */
	ConditionVariableCancelSleep();

	MyProc->localDistribXactData.state = LOCALDISTRIBXACT_STATE_NONE;
    MyProc->mppLocalProcessSerial = 0;
	MyProc->mppSessionId = InvalidGpSessionId;
    MyProc->mppIsWriter = false;
	MyProc->pid = 0;
	/* Cancel any pending condition variable sleep, too */
	ConditionVariableCancelSleep();

	/* Make sure active replication slots are released */
	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	/* Also cleanup all the temporary slots. */
	ReplicationSlotCleanup();

	/*
	 * Detach from any lock group of which we are a member.  If the leader
	 * exist before all other group members, it's PGPROC will remain allocated
	 * until the last group process exits; that process must return the
	 * leader's PGPROC to the appropriate list.
	 */
	if (MyProc->lockGroupLeader != NULL)
	{
		PGPROC	   *leader = MyProc->lockGroupLeader;
		LWLock	   *leader_lwlock = LockHashPartitionLockByProc(leader);

		LWLockAcquire(leader_lwlock, LW_EXCLUSIVE);
		Assert(!dlist_is_empty(&leader->lockGroupMembers));
		dlist_delete(&MyProc->lockGroupLink);
		if (dlist_is_empty(&leader->lockGroupMembers))
		{
			leader->lockGroupLeader = NULL;
			if (leader != MyProc)
			{
				procgloballist = leader->procgloballist;

				/* Leader exited first; return its PGPROC. */
				SpinLockAcquire(ProcStructLock);
				leader->links.next = (SHM_QUEUE *) *procgloballist;
				*procgloballist = leader;
				SpinLockRelease(ProcStructLock);
			}
		}
		else if (leader != MyProc)
			MyProc->lockGroupLeader = NULL;
		LWLockRelease(leader_lwlock);
	}

	/*
	 * Reset MyLatch to the process local one.  This is so that signal
	 * handlers et al can continue using the latch after the shared latch
	 * isn't ours anymore. After that clear MyProc and disown the shared
	 * latch.
	 */
	SwitchBackToLocalLatch();
	proc = MyProc;
	MyProc = NULL;
	lockHolderProcPtr = NULL;
	DisownLatch(&proc->procLatch);

	procgloballist = proc->procgloballist;
	SpinLockAcquire(ProcStructLock);

	/*
	 * If we're still a member of a locking group, that means we're a leader
	 * which has somehow exited before its children.  The last remaining child
	 * will release our PGPROC.  Otherwise, release it now.
	 */
	if (proc->lockGroupLeader == NULL)
	{
		/* Since lockGroupLeader is NULL, lockGroupMembers should be empty. */
		Assert(dlist_is_empty(&proc->lockGroupMembers));

		/* Return PGPROC structure (and semaphore) to appropriate freelist */
		proc->links.next = (SHM_QUEUE *) *procgloballist;
		*procgloballist = proc;
	}

	/* Update shared estimate of spins_per_delay */
	update_spins_per_delay();

	SpinLockRelease(ProcStructLock);

	/*
	 * This process is no longer present in shared memory in any meaningful
	 * way, so tell the postmaster we've cleaned up acceptably well. (XXX
	 * autovac launcher should be included here someday)
	 */
	if (IsUnderPostmaster && !IsAutoVacuumLauncherProcess()
		&& MyPMChildSlot > 0)
		MarkPostmasterChildInactive();

	/* wake autovac launcher if needed -- see comments in FreeWorkerInfo */
	if (AutovacuumLauncherPid != 0)
		kill(AutovacuumLauncherPid, SIGUSR2);
}

/*
 * AuxiliaryProcKill() -- Cut-down version of ProcKill for auxiliary
 *		processes (bgwriter, etc).  The PGPROC and sema are not released, only
 *		marked as not-in-use.
 */
static void
AuxiliaryProcKill(int code, Datum arg)
{
	int			proctype = DatumGetInt32(arg);
	PGPROC	   *auxproc PG_USED_FOR_ASSERTS_ONLY;
	PGPROC	   *proc;

	Assert(proctype >= 0 && proctype < NUM_AUXILIARY_PROCS);

	/* not safe if forked by system(), etc. */
	if (MyProc->pid != (int) getpid())
		elog(PANIC, "AuxiliaryProcKill() called in child process");

	auxproc = &AuxiliaryProcs[proctype];

	Assert(MyProc == auxproc);

	/* Release any LW locks I am holding (see notes above) */
	LWLockReleaseAll();

	/* Cancel any pending condition variable sleep, too */
	ConditionVariableCancelSleep();

	/*
	 * Reset MyLatch to the process local one.  This is so that signal
	 * handlers et al can continue using the latch after the shared latch
	 * isn't ours anymore. After that clear MyProc and disown the shared
	 * latch.
	 */
	SwitchBackToLocalLatch();
	proc = MyProc;
	MyProc = NULL;
	lockHolderProcPtr = NULL;
	DisownLatch(&proc->procLatch);

	SpinLockAcquire(ProcStructLock);

	/* Mark auxiliary proc no longer in use */
	proc->pid = 0;

	/* Update shared estimate of spins_per_delay */
	update_spins_per_delay();

	SpinLockRelease(ProcStructLock);
}

/*
 * AuxiliaryPidGetProc -- get PGPROC for an auxiliary process
 * given its PID
 *
 * Returns NULL if not found.
 */
PGPROC *
AuxiliaryPidGetProc(int pid)
{
	PGPROC	   *result = NULL;
	int			index;

	if (pid == 0)				/* never match dummy PGPROCs */
		return NULL;

	for (index = 0; index < NUM_AUXILIARY_PROCS; index++)
	{
		PGPROC	   *proc = &AuxiliaryProcs[index];

		if (proc->pid == pid)
		{
			result = proc;
			break;
		}
	}
	return result;
}

/*
 * ProcQueue package: routines for putting processes to sleep
 *		and  waking them up
 */

/*
 * ProcQueueAlloc -- alloc/attach to a shared memory process queue
 *
 * Returns: a pointer to the queue
 * Side Effects: Initializes the queue if it wasn't there before
 */
#ifdef NOT_USED
PROC_QUEUE *
ProcQueueAlloc(const char *name)
{
	PROC_QUEUE *queue;
	bool		found;

	queue = (PROC_QUEUE *)
		ShmemInitStruct(name, sizeof(PROC_QUEUE), &found);

	if (!found)
		ProcQueueInit(queue);

	return queue;
}
#endif

/*
 * ProcQueueInit -- initialize a shared memory process queue
 */
void
ProcQueueInit(PROC_QUEUE *queue)
{
	SHMQueueInit(&(queue->links));
	queue->size = 0;
}


/*
 * ProcSleep -- put a process to sleep on the specified lock
 *
 * Caller must have set MyProc->heldLocks to reflect locks already held
 * on the lockable object by this process (under all XIDs).
 *
 * The lock table's partition lock must be held at entry, and will be held
 * at exit.
 *
 * Result: STATUS_OK if we acquired the lock, STATUS_ERROR if not (deadlock).
 *
 * ASSUME: that no one will fiddle with the queue until after
 *		we release the partition lock.
 *
 * NOTES: The process queue is now a priority queue for locking.
 */
int
ProcSleep(LOCALLOCK *locallock, LockMethod lockMethodTable)
{
	LOCKMODE	lockmode = locallock->tag.mode;
	LOCK	   *lock = locallock->lock;
	PROCLOCK   *proclock = locallock->proclock;
	uint32		hashcode = locallock->hashcode;
	LWLock	   *partitionLock = LockHashPartitionLock(hashcode);
	PROC_QUEUE *waitQueue = &(lock->waitProcs);
	LOCKMASK	myHeldLocks = MyProc->heldLocks;
	bool		early_deadlock = false;
	bool		allow_autovacuum_cancel = true;
	int			myWaitStatus;
	PGPROC	   *proc;
	PGPROC	   *leader = MyProc->lockGroupLeader;
	int			i;

	/*
	 * If group locking is in use, locks held by members of my locking group
	 * need to be included in myHeldLocks.
	 */
	if (leader != NULL)
	{
		SHM_QUEUE  *procLocks = &(lock->procLocks);
		PROCLOCK   *otherproclock;

		otherproclock = (PROCLOCK *)
			SHMQueueNext(procLocks, procLocks, offsetof(PROCLOCK, lockLink));
		while (otherproclock != NULL)
		{
			if (otherproclock->groupLeader == leader)
				myHeldLocks |= otherproclock->holdMask;
			otherproclock = (PROCLOCK *)
				SHMQueueNext(procLocks, &otherproclock->lockLink,
							 offsetof(PROCLOCK, lockLink));
		}
	}

	/*
	 * Determine where to add myself in the wait queue.
	 *
	 * Normally I should go at the end of the queue.  However, if I already
	 * hold locks that conflict with the request of any previous waiter, put
	 * myself in the queue just in front of the first such waiter. This is not
	 * a necessary step, since deadlock detection would move me to before that
	 * waiter anyway; but it's relatively cheap to detect such a conflict
	 * immediately, and avoid delaying till deadlock timeout.
	 *
	 * Special case: if I find I should go in front of some waiter, check to
	 * see if I conflict with already-held locks or the requests before that
	 * waiter.  If not, then just grant myself the requested lock immediately.
	 * This is the same as the test for immediate grant in LockAcquire, except
	 * we are only considering the part of the wait queue before my insertion
	 * point.
	 */
	if (myHeldLocks != 0)
	{
		LOCKMASK	aheadRequests = 0;

		proc = (PGPROC *) waitQueue->links.next;
		for (i = 0; i < waitQueue->size; i++)
		{
			/*
			 * If we're part of the same locking group as this waiter, its
			 * locks neither conflict with ours nor contribute to
			 * aheadRequests.
			 */
			if (leader != NULL && leader == proc->lockGroupLeader)
			{
				proc = (PGPROC *) proc->links.next;
				continue;
			}
			/* Must he wait for me? */
			if (lockMethodTable->conflictTab[proc->waitLockMode] & myHeldLocks)
			{
				/* Must I wait for him ? */
				if (lockMethodTable->conflictTab[lockmode] & proc->heldLocks)
				{
					/*
					 * Yes, so we have a deadlock.  Easiest way to clean up
					 * correctly is to call RemoveFromWaitQueue(), but we
					 * can't do that until we are *on* the wait queue. So, set
					 * a flag to check below, and break out of loop.  Also,
					 * record deadlock info for later message.
					 */
					RememberSimpleDeadLock(MyProc, lockmode, lock, proc);
					early_deadlock = true;
					break;
				}
				/* I must go before this waiter.  Check special case. */
				if ((lockMethodTable->conflictTab[lockmode] & aheadRequests) == 0 &&
					LockCheckConflicts(lockMethodTable,
									   lockmode,
									   lock,
									   proclock) == STATUS_OK)
				{
					/* Skip the wait and just grant myself the lock. */
					GrantLock(lock, proclock, lockmode);
					GrantAwaitedLock();
					return STATUS_OK;
				}
				/* Break out of loop to put myself before him */
				break;
			}
			/* Nope, so advance to next waiter */
			aheadRequests |= LOCKBIT_ON(proc->waitLockMode);
			proc = (PGPROC *) proc->links.next;
		}

		/*
		 * If we fall out of loop normally, proc points to waitQueue head, so
		 * we will insert at tail of queue as desired.
		 */
	}
	else
	{
		/* I hold no locks, so I can't push in front of anyone. */
		proc = (PGPROC *) &(waitQueue->links);
	}

	/*
	 * Insert self into queue, ahead of the given proc (or at tail of queue).
	 */
	SHMQueueInsertBefore(&(proc->links), &(MyProc->links));
	waitQueue->size++;

	lock->waitMask |= LOCKBIT_ON(lockmode);

	/* Set up wait information in PGPROC object, too */
	MyProc->waitLock = lock;
	MyProc->waitProcLock = proclock;
	MyProc->waitLockMode = lockmode;

	MyProc->waitStatus = STATUS_WAITING;

	/*
	 * If we detected deadlock, give up without waiting.  This must agree with
	 * CheckDeadLock's recovery code.
	 */
	if (early_deadlock)
	{
		RemoveFromWaitQueue(MyProc, hashcode);
		return STATUS_ERROR;
	}

	/* mark that we are waiting for a lock */
	lockAwaited = locallock;

	/*
	 * Release the lock table's partition lock.
	 *
	 * NOTE: this may also cause us to exit critical-section state, possibly
	 * allowing a cancel/die interrupt to be accepted. This is OK because we
	 * have recorded the fact that we are waiting for a lock, and so
	 * LockErrorCleanup will clean up if cancel/die happens.
	 */
	LWLockRelease(partitionLock);

	/*
	 * Also, now that we will successfully clean up after an ereport, it's
	 * safe to check to see if there's a buffer pin deadlock against the
	 * Startup process.  Of course, that's only necessary if we're doing Hot
	 * Standby and are not the Startup process ourselves.
	 */
	if (RecoveryInProgress() && !InRecovery)
		CheckRecoveryConflictDeadlock();

	/* Reset deadlock_state before enabling the timeout handler */
	deadlock_state = DS_NOT_YET_CHECKED;
	got_deadlock_timeout = false;

	/*
	 * Set timer so we can wake up after awhile and check for a deadlock. If a
	 * deadlock is detected, the handler sets MyProc->waitStatus =
	 * STATUS_ERROR, allowing us to know that we must report failure rather
	 * than success.
	 *
	 * By delaying the check until we've waited for a bit, we can avoid
	 * running the rather expensive deadlock-check code in most cases.
	 *
	 * If LockTimeout is set, also enable the timeout for that.  We can save a
	 * few cycles by enabling both timeout sources in one call.
	 *
	 * If InHotStandby we set lock waits slightly later for clarity with other
	 * code.
	 */
	if (!InHotStandby)
	{
		if (LockTimeout > 0)
		{
			EnableTimeoutParams timeouts[2];

			timeouts[0].id = DEADLOCK_TIMEOUT;
			timeouts[0].type = TMPARAM_AFTER;
			timeouts[0].delay_ms = DeadlockTimeout;
			timeouts[1].id = LOCK_TIMEOUT;
			timeouts[1].type = TMPARAM_AFTER;
			timeouts[1].delay_ms = LockTimeout;
			enable_timeouts(timeouts, 2);
		}
		else
			enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);
	}

	/*
	 * If somebody wakes us between LWLockRelease and WaitLatch, the latch
	 * will not wait. But a set latch does not necessarily mean that the lock
	 * is free now, as there are many other sources for latch sets than
	 * somebody releasing the lock.
	 *
	 * We process interrupts whenever the latch has been set, so cancel/die
	 * interrupts are processed quickly. This means we must not mind losing
	 * control to a cancel/die interrupt here.  We don't, because we have no
	 * shared-state-change work to do after being granted the lock (the
	 * grantor did it all).  We do have to worry about canceling the deadlock
	 * timeout and updating the locallock table, but if we lose control to an
	 * error, LockErrorCleanup will fix that up.
	 */
	do
	{
		if (InHotStandby)
		{
			/* Set a timer and wait for that or for the Lock to be granted */
			ResolveRecoveryConflictWithLock(locallock->tag.lock);
		}
		else
		{
			(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
							 PG_WAIT_LOCK | locallock->tag.lock.locktag_type);
			ResetLatch(MyLatch);
			/* check for deadlocks first, as that's probably log-worthy */
			if (got_deadlock_timeout)
			{
				CheckDeadLock();
				got_deadlock_timeout = false;
			}
			CHECK_FOR_INTERRUPTS();
		}

		/*
		 * waitStatus could change from STATUS_WAITING to something else
		 * asynchronously.  Read it just once per loop to prevent surprising
		 * behavior (such as missing log messages).
		 */
		myWaitStatus = *((volatile int *) &MyProc->waitStatus);

		/*
		 * If we are not deadlocked, but are waiting on an autovacuum-induced
		 * task, send a signal to interrupt it.
		 */
		if (deadlock_state == DS_BLOCKED_BY_AUTOVACUUM && allow_autovacuum_cancel)
		{
			PGPROC	   *autovac = GetBlockingAutoVacuumPgproc();
			PGXACT	   *autovac_pgxact = &ProcGlobal->allPgXact[autovac->pgprocno];

			LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

			/*
			 * Only do it if the worker is not working to protect against Xid
			 * wraparound.
			 */
			if ((autovac_pgxact->vacuumFlags & PROC_IS_AUTOVACUUM) &&
				!(autovac_pgxact->vacuumFlags & PROC_VACUUM_FOR_WRAPAROUND))
			{
				int			pid = autovac->pid;
				StringInfoData locktagbuf;
				StringInfoData logbuf;	/* errdetail for server log */

				initStringInfo(&locktagbuf);
				initStringInfo(&logbuf);
				DescribeLockTag(&locktagbuf, &lock->tag);
				appendStringInfo(&logbuf,
								 _("Process %d waits for %s on %s."),
								 MyProcPid,
								 GetLockmodeName(lock->tag.locktag_lockmethodid,
												 lockmode),
								 locktagbuf.data);

				/* release lock as quickly as possible */
				LWLockRelease(ProcArrayLock);

				/* send the autovacuum worker Back to Old Kent Road */
				ereport(DEBUG1,
						(errmsg("sending cancel to blocking autovacuum PID %d",
								pid),
						 errdetail_log("%s", logbuf.data)));

				if (kill(pid, SIGINT) < 0)
				{
					/*
					 * There's a race condition here: once we release the
					 * ProcArrayLock, it's possible for the autovac worker to
					 * close up shop and exit before we can do the kill().
					 * Therefore, we do not whinge about no-such-process.
					 * Other errors such as EPERM could conceivably happen if
					 * the kernel recycles the PID fast enough, but such cases
					 * seem improbable enough that it's probably best to issue
					 * a warning if we see some other errno.
					 */
					if (errno != ESRCH)
						ereport(WARNING,
								(errmsg("could not send signal to process %d: %m",
										pid)));
				}

				pfree(logbuf.data);
				pfree(locktagbuf.data);
			}
			else
				LWLockRelease(ProcArrayLock);

			/* prevent signal from being sent again more than once */
			allow_autovacuum_cancel = false;
		}

		/*
		 * If awoken after the deadlock check interrupt has run, and
		 * log_lock_waits is on, then report about the wait.
		 */
		if (log_lock_waits && deadlock_state != DS_NOT_YET_CHECKED)
		{
			StringInfoData buf,
						lock_waiters_sbuf,
						lock_holders_sbuf;
			const char *modename;
			long		secs;
			int			usecs;
			long		msecs;
			SHM_QUEUE  *procLocks;
			PROCLOCK   *proclock;
			bool		first_holder = true,
						first_waiter = true;
			int			lockHoldersNum = 0;

			initStringInfo(&buf);
			initStringInfo(&lock_waiters_sbuf);
			initStringInfo(&lock_holders_sbuf);

			DescribeLockTag(&buf, &locallock->tag.lock);
			modename = GetLockmodeName(locallock->tag.lock.locktag_lockmethodid,
									   lockmode);
			TimestampDifference(get_timeout_start_time(DEADLOCK_TIMEOUT),
								GetCurrentTimestamp(),
								&secs, &usecs);
			msecs = secs * 1000 + usecs / 1000;
			usecs = usecs % 1000;

			/*
			 * we loop over the lock's procLocks to gather a list of all
			 * holders and waiters. Thus we will be able to provide more
			 * detailed information for lock debugging purposes.
			 *
			 * lock->procLocks contains all processes which hold or wait for
			 * this lock.
			 */

			LWLockAcquire(partitionLock, LW_SHARED);

			procLocks = &(lock->procLocks);
			proclock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
												 offsetof(PROCLOCK, lockLink));

			while (proclock)
			{
				/*
				 * we are a waiter if myProc->waitProcLock == proclock; we are
				 * a holder if it is NULL or something different
				 */
				if (proclock->tag.myProc->waitProcLock == proclock)
				{
					if (first_waiter)
					{
						appendStringInfo(&lock_waiters_sbuf, "%d",
										 proclock->tag.myProc->pid);
						first_waiter = false;
					}
					else
						appendStringInfo(&lock_waiters_sbuf, ", %d",
										 proclock->tag.myProc->pid);
				}
				else
				{
					if (first_holder)
					{
						appendStringInfo(&lock_holders_sbuf, "%d",
										 proclock->tag.myProc->pid);
						first_holder = false;
					}
					else
						appendStringInfo(&lock_holders_sbuf, ", %d",
										 proclock->tag.myProc->pid);

					lockHoldersNum++;
				}

				proclock = (PROCLOCK *) SHMQueueNext(procLocks, &proclock->lockLink,
													 offsetof(PROCLOCK, lockLink));
			}

			LWLockRelease(partitionLock);

			if (deadlock_state == DS_SOFT_DEADLOCK)
				ereport(LOG,
						(errmsg("process %d avoided deadlock for %s on %s by rearranging queue order after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
											   "Processes holding the lock: %s. Wait queue: %s.",
											   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			else if (deadlock_state == DS_HARD_DEADLOCK)
			{
				/*
				 * This message is a bit redundant with the error that will be
				 * reported subsequently, but in some cases the error report
				 * might not make it to the log (eg, if it's caught by an
				 * exception handler), and we want to ensure all long-wait
				 * events get logged.
				 */
				ereport(LOG,
						(errmsg("process %d detected deadlock while waiting for %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
											   "Processes holding the lock: %s. Wait queue: %s.",
											   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			}

			if (myWaitStatus == STATUS_WAITING)
				ereport(LOG,
						(errmsg("process %d still waiting for %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
											   "Processes holding the lock: %s. Wait queue: %s.",
											   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			else if (myWaitStatus == STATUS_OK)
				ereport(LOG,
						(errmsg("process %d acquired %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs)));
			else
			{
				Assert(myWaitStatus == STATUS_ERROR);

				/*
				 * Currently, the deadlock checker always kicks its own
				 * process, which means that we'll only see STATUS_ERROR when
				 * deadlock_state == DS_HARD_DEADLOCK, and there's no need to
				 * print redundant messages.  But for completeness and
				 * future-proofing, print a message if it looks like someone
				 * else kicked us off the lock.
				 */
				if (deadlock_state != DS_HARD_DEADLOCK)
					ereport(LOG,
							(errmsg("process %d failed to acquire %s on %s after %ld.%03d ms",
									MyProcPid, modename, buf.data, msecs, usecs),
							 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
												   "Processes holding the lock: %s. Wait queue: %s.",
												   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			}

			/*
			 * At this point we might still need to wait for the lock. Reset
			 * state so we don't print the above messages again.
			 */
			deadlock_state = DS_NO_DEADLOCK;

			pfree(buf.data);
			pfree(lock_holders_sbuf.data);
			pfree(lock_waiters_sbuf.data);
		}
	} while (myWaitStatus == STATUS_WAITING);

	/*
	 * Disable the timers, if they are still running.  As in LockErrorCleanup,
	 * we must preserve the LOCK_TIMEOUT indicator flag: if a lock timeout has
	 * already caused QueryCancelPending to become set, we want the cancel to
	 * be reported as a lock timeout, not a user cancel.
	 */
	if (!InHotStandby)
	{
		if (LockTimeout > 0)
		{
			DisableTimeoutParams timeouts[2];

			timeouts[0].id = DEADLOCK_TIMEOUT;
			timeouts[0].keep_indicator = false;
			timeouts[1].id = LOCK_TIMEOUT;
			timeouts[1].keep_indicator = true;
			disable_timeouts(timeouts, 2);
		}
		else
			disable_timeout(DEADLOCK_TIMEOUT, false);
	}

	/*
	 * Re-acquire the lock table's partition lock.  We have to do this to hold
	 * off cancel/die interrupts before we can mess with lockAwaited (else we
	 * might have a missed or duplicated locallock update).
	 */
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	/*
	 * We no longer want LockErrorCleanup to do anything.
	 */
	lockAwaited = NULL;

	/*
	 * If we got the lock, be sure to remember it in the locallock table.
	 */
	if (MyProc->waitStatus == STATUS_OK)
		GrantAwaitedLock();

	/*
	 * We don't have to do anything else, because the awaker did all the
	 * necessary update of the lock table and MyProc.
	 */
	return MyProc->waitStatus;
}


/*
 * ProcWakeup -- wake up a process by setting its latch.
 *
 *	 Also remove the process from the wait queue and set its links invalid.
 *	 RETURN: the next process in the wait queue.
 *
 * The appropriate lock partition lock must be held by caller.
 *
 * XXX: presently, this code is only used for the "success" case, and only
 * works correctly for that case.  To clean up in failure case, would need
 * to twiddle the lock's request counts too --- see RemoveFromWaitQueue.
 * Hence, in practice the waitStatus parameter must be STATUS_OK.
 */
PGPROC *
ProcWakeup(PGPROC *proc, int waitStatus)
{
	PGPROC	   *retProc;

	/* Proc should be sleeping ... */
	if (proc->links.prev == NULL ||
		proc->links.next == NULL)
		return NULL;
	Assert(proc->waitStatus == STATUS_WAITING);

	/* Save next process before we zap the list link */
	retProc = (PGPROC *) proc->links.next;

	/* Remove process from wait queue */
	SHMQueueDelete(&(proc->links));
	(proc->waitLock->waitProcs.size)--;

	/* Clean up process' state and pass it the ok/fail signal */
	proc->waitLock = NULL;
	proc->waitProcLock = NULL;
	proc->waitStatus = waitStatus;

	/* And awaken it */
	SetLatch(&proc->procLatch);

	return retProc;
}

/*
 * ProcLockWakeup -- routine for waking up processes when a lock is
 *		released (or a prior waiter is aborted).  Scan all waiters
 *		for lock, waken any that are no longer blocked.
 *
 * The appropriate lock partition lock must be held by caller.
 */
void
ProcLockWakeup(LockMethod lockMethodTable, LOCK *lock)
{
	PROC_QUEUE *waitQueue = &(lock->waitProcs);
	int			queue_size = waitQueue->size;
	PGPROC	   *proc;
	LOCKMASK	aheadRequests = 0;

	Assert(queue_size >= 0);

	if (queue_size == 0)
		return;

	proc = (PGPROC *) waitQueue->links.next;

	while (queue_size-- > 0)
	{
		LOCKMODE	lockmode = proc->waitLockMode;

		/*
		 * Waken if (a) doesn't conflict with requests of earlier waiters, and
		 * (b) doesn't conflict with already-held locks.
		 */
		if ((lockMethodTable->conflictTab[lockmode] & aheadRequests) == 0 &&
			LockCheckConflicts(lockMethodTable,
							   lockmode,
							   lock,
							   proc->waitProcLock) == STATUS_OK)
		{
			/* OK to waken */
			GrantLock(lock, proc->waitProcLock, lockmode);
			proc = ProcWakeup(proc, STATUS_OK);

			/*
			 * ProcWakeup removes proc from the lock's waiting process queue
			 * and returns the next proc in chain; don't use proc's next-link,
			 * because it's been cleared.
			 */
		}
		else
		{
			/*
			 * Cannot wake this guy. Remember his request for later checks.
			 */
			aheadRequests |= LOCKBIT_ON(lockmode);
			proc = (PGPROC *) proc->links.next;
		}
	}

	Assert(waitQueue->size >= 0);
}

/*
 * CheckDeadLock
 *
 * We only get to this routine, if DEADLOCK_TIMEOUT fired while waiting for a
 * lock to be released by some other process.  Check if there's a deadlock; if
 * not, just return.  (But signal ProcSleep to log a message, if
 * log_lock_waits is true.)  If we have a real deadlock, remove ourselves from
 * the lock's wait queue and signal an error to ProcSleep.
 */
static void
CheckDeadLock(void)
{
	int			i;

	/*
	 * This check was added in GPDB a long time ago. Not sure if it's still
	 * needed, but seems like it can't hurt.
	 *
	 * From old pre-open sourcing git repository:
	 * commit d628fac161d0536b344348927915335bbcd38c1a
	 * Date:   Wed Aug 19 03:26:36 2015 -0400
	 *
	 *    [JIRA: MPP-25646] Add proc_exit_inprogress check in handle_sig_alarm.
	 *
	 *    If SIGALRM happens in the middle if handling SIGTERM, there are risks causing
	 *    SIGSEGV, for instance, double free gang, or accessing MyProc while it was freed
	 *    already. The solution is to ignore SIGALRM while we are dying.
	 */
	if (proc_exit_inprogress)
		return;

	/*
	 * Acquire exclusive lock on the entire shared lock data structures. Must
	 * grab LWLocks in partition-number order to avoid LWLock deadlock.
	 *
	 * Note that the deadlock check interrupt had better not be enabled
	 * anywhere that this process itself holds lock partition locks, else this
	 * will wait forever.  Also note that LWLockAcquire creates a critical
	 * section, so that this routine cannot be interrupted by cancel/die
	 * interrupts.
	 */
	for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
		LWLockAcquire(LockHashPartitionLockByIndex(i), LW_EXCLUSIVE);

	/*
	 * Check to see if we've been awoken by anyone in the interim.
	 *
	 * If we have, we can return and resume our transaction -- happy day.
	 * Before we are awoken the process releasing the lock grants it to us so
	 * we know that we don't have to wait anymore.
	 *
	 * We check by looking to see if we've been unlinked from the wait queue.
	 * This is safe because we hold the lock partition lock.
	 */
	if (MyProc->links.prev == NULL ||
		MyProc->links.next == NULL)
		goto check_done;

#ifdef LOCK_DEBUG
	if (Debug_deadlocks)
		DumpAllLocks();
#endif

	/* Run the deadlock check, and set deadlock_state for use by ProcSleep */
	deadlock_state = DeadLockCheck(MyProc);

	if (deadlock_state == DS_HARD_DEADLOCK)
	{
		/*
		 * Oops.  We have a deadlock.
		 *
		 * Get this process out of wait state.	(Note: we could do this more
		 * efficiently by relying on lockAwaited, but use this coding to
		 * preserve the flexibility to kill some other transaction than the
		 * one detecting the deadlock.)
		 *
		 * RemoveFromWaitQueue sets MyProc->waitStatus to STATUS_ERROR, so
		 * ProcSleep will report an error after we return from the signal
		 * handler.
		 */
		Assert(MyProc->waitLock != NULL);
		if (Gp_role == GP_ROLE_DISPATCH && IsResQueueEnabled() &&
			LOCK_LOCKMETHOD(*(MyProc->waitLock)) == RESOURCE_LOCKMETHOD)
		{
			/*
			 * If there are no other locked portals resident in this backend
			 * (i.e. nLocks == 0), lockAwaited's lock/proclock pointers are dangling
			 * after the following call to ResRemoveFromWaitQueue(). So clean up the
			 * locallock as well, to avoid de-referencing them in the eventual
			 * ResLockRelease() in ResLockPortal()/ResLockUtilityPortal().
			 *
			 * If there are other locked portals resident in this backend
			 * (i.e. nLocks > 0), as always, the lock and proclock cannot be cleaned
			 * up now. Thus, defer the cleanup of the locallock.
			 */
			if (MyProc->waitProcLock->nLocks == 0)
				RemoveLocalLock(lockAwaited);

			ResRemoveFromWaitQueue(MyProc,
								   LockTagHashCode(&(MyProc->waitLock->tag)));
		}
		else
		{
			RemoveFromWaitQueue(MyProc, LockTagHashCode(&(MyProc->waitLock->tag)));
		}

		/*
		 * We're done here.  Transaction abort caused by the error that
		 * ProcSleep will raise will cause any other locks we hold to be
		 * released, thus allowing other processes to wake up; we don't need
		 * to do that here.  NOTE: an exception is that releasing locks we
		 * hold doesn't consider the possibility of waiters that were blocked
		 * behind us on the lock we just failed to get, and might now be
		 * wakable because we're not in front of them anymore.  However,
		 * RemoveFromWaitQueue took care of waking up any such processes.
		 */
	}

	/*
	 * And release locks.  We do this in reverse order for two reasons: (1)
	 * Anyone else who needs more than one of the locks will be trying to lock
	 * them in increasing order; we don't want to release the other process
	 * until it can get all the locks it needs. (2) This avoids O(N^2)
	 * behavior inside LWLockRelease.
	 */
check_done:
	for (i = NUM_LOCK_PARTITIONS; --i >= 0;)
		LWLockRelease(LockHashPartitionLockByIndex(i));
}

/*
 * CheckDeadLockAlert - Handle the expiry of deadlock_timeout.
 *
 * NB: Runs inside a signal handler, be careful.
 */
void
CheckDeadLockAlert(void)
{
	int			save_errno = errno;

	got_deadlock_timeout = true;

	/*
	 * Have to set the latch again, even if handle_sig_alarm already did. Back
	 * then got_deadlock_timeout wasn't yet set... It's unlikely that this
	 * ever would be a problem, but setting a set latch again is cheap.
	 *
	 * Note that, when this function runs inside procsignal_sigusr1_handler(),
	 * the handler function sets the latch again after the latch is set here.
	 */
	SetLatch(MyLatch);
	errno = save_errno;
}

/*
 * ProcWaitForSignal - wait for a signal from another backend.
 *
 * As this uses the generic process latch the caller has to be robust against
 * unrelated wakeups: Always check that the desired state has occurred, and
 * wait again if not.
 */
void
ProcWaitForSignal(uint32 wait_event_info)
{
	(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
					 wait_event_info);
	ResetLatch(MyLatch);
	CHECK_FOR_INTERRUPTS();
}

/*
 * ProcSendSignal - send a signal to a backend identified by PID
 */
void
ProcSendSignal(int pid)
{
	PGPROC	   *proc = NULL;

	if (RecoveryInProgress())
	{
		SpinLockAcquire(ProcStructLock);

		/*
		 * Check to see whether it is the Startup process we wish to signal.
		 * This call is made by the buffer manager when it wishes to wake up a
		 * process that has been waiting for a pin in so it can obtain a
		 * cleanup lock using LockBufferForCleanup(). Startup is not a normal
		 * backend, so BackendPidGetProc() will not return any pid at all. So
		 * we remember the information for this special case.
		 */
		if (pid == ProcGlobal->startupProcPid)
			proc = ProcGlobal->startupProc;

		SpinLockRelease(ProcStructLock);
	}

	if (proc == NULL)
		proc = BackendPidGetProc(pid);

	if (proc != NULL)
	{
		SetLatch(&proc->procLatch);
	}
}

/*
 * ResProcSleep -- put a process to sleep (that is waiting for a resource lock).
 *
 * Notes:
 * 	Locktable's mainLock must be held at entry, and will be held
 * 	at exit.
 *
 *	This is merely a version of ProcSleep modified for resource locks.
 *	The logic here could have been merged into ProcSleep, however it was
 *	requested to keep as much as possible of this resource lock code 
 *	separate from its standard lock relatives - in the interest of not
 *	introducing new bugs or performance regressions into the lock code.
 */
int
ResProcSleep(LOCKMODE lockmode, LOCALLOCK *locallock, void *incrementSet)
{
	LOCK	   *lock = locallock->lock;
	PROCLOCK   *proclock = locallock->proclock;
	PROC_QUEUE	*waitQueue = &(lock->waitProcs);
	int			myWaitStatus;
	PGPROC		*proc;
	uint32		hashcode = locallock->hashcode;
	LWLockId	partitionLock = LockHashPartitionLock(hashcode);

	/*
	 * Don't check my held locks, as we just add at the end of the queue.
	 */
	proc = (PGPROC *) &(waitQueue->links);
	SHMQueueInsertBefore(&(proc->links), &(MyProc->links));
	waitQueue->size++;

	lock->waitMask |= LOCKBIT_ON(lockmode);

	/*
	 * reflect this in PGPROC object, too.
	 */
	MyProc->waitLock = lock;
	MyProc->waitProcLock = (PROCLOCK *) proclock;
	MyProc->waitLockMode = lockmode;

	MyProc->waitStatus = STATUS_WAITING;	/* initialize result for error */

	/* Mark that we are waiting for a lock */
	lockAwaited = locallock;

	/* Ok to wait.*/
	LWLockRelease(partitionLock);

	/*
	 * Free/destroy idle gangs as we are going to sleep.
	 */
	if (ResourceCleanupIdleGangs)
		cdbcomponent_cleanupIdleQEs(false);

	/* Reset deadlock_state before enabling the timeout handler */
	deadlock_state = DS_NOT_YET_CHECKED;
	got_deadlock_timeout = false;

	if (LockTimeout > 0)
	{
		EnableTimeoutParams timeouts[2];

		timeouts[0].id = DEADLOCK_TIMEOUT;
		timeouts[0].type = TMPARAM_AFTER;
		timeouts[0].delay_ms = DeadlockTimeout;
		timeouts[1].id = LOCK_TIMEOUT;
		timeouts[1].type = TMPARAM_AFTER;
		timeouts[1].delay_ms = LockTimeout;
		enable_timeouts(timeouts, 2);
	}
	else
		enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);

	do
	{
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
					PG_WAIT_RESOURCE_QUEUE);
		ResetLatch(MyLatch);
		/* check for deadlocks first, as that's probably log-worthy */
		if (got_deadlock_timeout)
		{
			CheckDeadLock();
			got_deadlock_timeout = false;
		}
		CHECK_FOR_INTERRUPTS();

		/*
		 * waitStatus could change from STATUS_WAITING to something else
		 * asynchronously.  Read it just once per loop to prevent surprising
		 * behavior (such as missing log messages).
		 */
		myWaitStatus = *((volatile int *) &MyProc->waitStatus);

		/*
		 * If awoken after the deadlock check interrupt has run, and
		 * log_lock_waits is on, then report about the wait.
		 */
		if (log_lock_waits && deadlock_state != DS_NOT_YET_CHECKED)
		{
			StringInfoData buf,
						lock_waiters_sbuf,
						lock_holders_sbuf;
			const char	*modename;
			long		secs;
			int			usecs;
			long		msecs;
			SHM_QUEUE	*procLocks;
			PROCLOCK	*proclock;
			bool		first_holder = true,
						first_waiter = true;
			int			lockHoldersNum = 0;

			initStringInfo(&buf);
			initStringInfo(&lock_waiters_sbuf);
			initStringInfo(&lock_holders_sbuf);

			DescribeLockTag(&buf, &locallock->tag.lock);
			modename = GetLockmodeName(locallock->tag.lock.locktag_lockmethodid,
									   lockmode);
			TimestampDifference(get_timeout_start_time(DEADLOCK_TIMEOUT),
								GetCurrentTimestamp(),
								&secs, &usecs);
			msecs = secs * 1000 + usecs / 1000;
			usecs = usecs % 1000;

			/*
			 * we loop over the lock's procLocks to gather a list of all
			 * holders and waiters. Thus we will be able to provide more
			 * detailed information for lock debugging purposes.
			 *
			 * lock->procLocks contains all processes which hold or wait for
			 * this lock.
			 */
			LWLockAcquire(partitionLock, LW_SHARED);

			procLocks = &(lock->procLocks);
			proclock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
												 offsetof(PROCLOCK, lockLink));

			while (proclock)
			{
				/*
				 * we are a waiter if myProc->waitProcLock == proclock; we are
				 * a holder if it is NULL or something different
				 */
				if (proclock->tag.myProc->waitProcLock == proclock)
				{
					if (first_waiter)
					{
						appendStringInfo(&lock_waiters_sbuf, "%d",
									proclock->tag.myProc->pid);
						first_waiter = false;
					}
					else
						appendStringInfo(&lock_waiters_sbuf, ", %d",
										 proclock->tag.myProc->pid);
				}
				else
				{
					if (first_holder)
					{
						appendStringInfo(&lock_holders_sbuf, "%d",
										 proclock->tag.myProc->pid);
						first_holder = false;
					}
					else
						appendStringInfo(&lock_holders_sbuf, ", %d",
										 proclock->tag.myProc->pid);
					lockHoldersNum++;
				}

				proclock = (PROCLOCK *) SHMQueueNext(procLocks, &proclock->lockLink,
												offsetof(PROCLOCK, lockLink));
			}

			LWLockRelease(partitionLock);

			if (deadlock_state == DS_SOFT_DEADLOCK)
				ereport(LOG,
						(errmsg("process %d avoided deadlock for %s on %s by rearranging queue order after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
							"Processes holding the lock: %s. Wait queue: %s.",
												lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			else if (deadlock_state == DS_HARD_DEADLOCK)
			{
				/*
				 * This message is a bit redundant with the error that will be
				 * reported subsequently, but in some cases the error report
				 * might not make it to the log (eg, if it's caught by an
				 * exception handler), and we want to ensure all long-wait
				 * events get logged.
				 */
				ereport(LOG,
						(errmsg("process %d detected deadlock while waiting for %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
						   "Processes holding the lock: %s. Wait queue: %s.",
												lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			}

			if (myWaitStatus == STATUS_WAITING)
				ereport(LOG,
						(errmsg("process %d still waiting for %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
						   "Processes holding the lock: %s. Wait queue: %s.",
												lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			else if (myWaitStatus == STATUS_OK)
				ereport(LOG,
					(errmsg("process %d acquired %s on %s after %ld.%03d ms",
							MyProcPid, modename, buf.data, msecs, usecs)));
			else
			{
				Assert(myWaitStatus == STATUS_ERROR);

				/*
				 * Currently, the deadlock checker always kicks its own
				 * process, which means that we'll only see STATUS_ERROR when
				 * deadlock_state == DS_HARD_DEADLOCK, and there's no need to
				 * print redundant messages.  But for completeness and
				 * future-proofing, print a message if it looks like someone
				 * else kicked us off the lock.
				 */
				if (deadlock_state != DS_HARD_DEADLOCK)
					ereport(LOG,
							(errmsg("process %d failed to acquire %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
							 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
							"Processes holding the lock: %s. Wait queue: %s.",
													lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			}

			/*
			 * At this point we might still need to wait for the lock. Reset
			 * state so we don't print the above messages again.
			 */
			deadlock_state = DS_NO_DEADLOCK;

			pfree(buf.data);
			pfree(lock_holders_sbuf.data);
			pfree(lock_waiters_sbuf.data);
		}
	} while (myWaitStatus == STATUS_WAITING);

	if (LockTimeout > 0)
	{
		DisableTimeoutParams timeouts[2];

		timeouts[0].id = DEADLOCK_TIMEOUT;
		timeouts[0].keep_indicator = false;
		timeouts[1].id = LOCK_TIMEOUT;
		timeouts[1].keep_indicator = false;
		disable_timeouts(timeouts, 2);
	}
	else
		disable_timeout(DEADLOCK_TIMEOUT, false);

	/*
	 * Have been awakened, so continue.
	 */
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	/*
	 * We no longer want (Res)LockWaitCancel to do anything.
	 */
	lockAwaited = NULL;

	return MyProc->waitStatus;
}


/*
 * ResLockWaitCancel -- Cancel any pending wait for a resource lock, when 
 *	aborting a transaction.
 */
void
ResLockWaitCancel(void)
{
	LWLockId	partitionLock;
	DisableTimeoutParams timeouts[2];

	HOLD_INTERRUPTS();

	AbortStrongLockAcquire();

	/* Nothing to do if we weren't waiting for a lock */
	if (lockAwaited == NULL)
	{
		RESUME_INTERRUPTS();
		return;
	}

	/*
	 * Turn off the deadlock and lock timeout timers, if they are still
	 * running (see ResProcSleep).  Note we must preserve the LOCK_TIMEOUT
	 * indicator flag, since this function is executed before
	 * ProcessInterrupts when responding to SIGINT; else we'd lose the
	 * knowledge that the SIGINT came from a lock timeout and not an external
	 * source.
	 */
	timeouts[0].id = DEADLOCK_TIMEOUT;
	timeouts[0].keep_indicator = false;
	timeouts[1].id = LOCK_TIMEOUT;
	timeouts[1].keep_indicator = true;
	disable_timeouts(timeouts, 2);

	SIMPLE_FAULT_INJECTOR("res_lock_wait_cancel_before_partition_lock");

	/* Unlink myself from the wait queue, if on it  */
	partitionLock = LockHashPartitionLock(lockAwaited->hashcode);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	if (MyProc->links.next != NULL)
	{
		/* We could not have been granted the lock yet */
		Assert(MyProc->waitStatus == STATUS_WAITING);

		/* We should only be trying to cancel resource locks */
		Assert(LOCALLOCK_LOCKMETHOD(*lockAwaited) == RESOURCE_LOCKMETHOD);

		/*
		 * If there are no other locked portals resident in this backend
		 * (i.e. nLocks == 0), lockAwaited's lock/proclock pointers are dangling
		 * after the following call to ResRemoveFromWaitQueue(). So clean up the
		 * locallock as well, to avoid de-referencing them in the eventual
		 * ResLockRelease() in ResLockPortal()/ResLockUtilityPortal().
		 *
		 * If there are other locked portals resident in this backend
		 * (i.e. nLocks > 0), as always, the lock and proclock cannot be cleaned
		 * up now. Thus, defer the cleanup of the locallock.
		 */
		if (MyProc->waitProcLock->nLocks == 0)
			RemoveLocalLock(lockAwaited);

		ResRemoveFromWaitQueue(MyProc, lockAwaited->hashcode);
	}

	lockAwaited = NULL;

	LWLockRelease(partitionLock);

	RESUME_INTERRUPTS();
}

bool ProcCanSetMppSessionId(void)
{
	if (ProcGlobal == NULL || MyProc == NULL)
		return false;

	return true;
}


void ProcNewMppSessionId(int *newSessionId)
{
	Assert(newSessionId != NULL);

    *newSessionId = MyProc->mppSessionId =
		pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&ProcGlobal->mppLocalProcessCounter, 1);

    /*
     * Make sure that our SessionState entry correctly records our
     * new session id.
     */
    if (NULL != MySessionState)
    {
    	/* This should not happen outside of dispatcher on the coordinator */
    	Assert(IS_QUERY_DISPATCHER() && Gp_role == GP_ROLE_DISPATCH);

    	ereport(gp_sessionstate_loglevel, (errmsg("ProcNewMppSessionId: changing session id (old: %d, new: %d), pinCount: %d, activeProcessCount: %d",
    			MySessionState->sessionId, *newSessionId, MySessionState->pinCount, MySessionState->activeProcessCount), errprintstack(true)));

#ifdef USE_ASSERT_CHECKING
    	MySessionState->isModifiedSessionId = true;
#endif

    	MySessionState->sessionId = *newSessionId;
    }
}

/*
 * BecomeLockGroupLeader - designate process as lock group leader
 *
 * Once this function has returned, other processes can join the lock group
 * by calling BecomeLockGroupMember.
 */
void
BecomeLockGroupLeader(void)
{
	LWLock	   *leader_lwlock;

	/* If we already did it, we don't need to do it again. */
	if (MyProc->lockGroupLeader == MyProc)
		return;

	/* We had better not be a follower. */
	Assert(MyProc->lockGroupLeader == NULL);

	/* Create single-member group, containing only ourselves. */
	leader_lwlock = LockHashPartitionLockByProc(MyProc);
	LWLockAcquire(leader_lwlock, LW_EXCLUSIVE);
	MyProc->lockGroupLeader = MyProc;
	dlist_push_head(&MyProc->lockGroupMembers, &MyProc->lockGroupLink);
	LWLockRelease(leader_lwlock);
}

/*
 * BecomeLockGroupMember - designate process as lock group member
 *
 * This is pretty straightforward except for the possibility that the leader
 * whose group we're trying to join might exit before we manage to do so;
 * and the PGPROC might get recycled for an unrelated process.  To avoid
 * that, we require the caller to pass the PID of the intended PGPROC as
 * an interlock.  Returns true if we successfully join the intended lock
 * group, and false if not.
 */
bool
BecomeLockGroupMember(PGPROC *leader, int pid)
{
	LWLock	   *leader_lwlock;
	bool		ok = false;

	/* Group leader can't become member of group */
	Assert(MyProc != leader);

	/* Can't already be a member of a group */
	Assert(MyProc->lockGroupLeader == NULL);

	/* PID must be valid. */
	Assert(pid != 0);

	/*
	 * Get lock protecting the group fields.  Note LockHashPartitionLockByProc
	 * accesses leader->pgprocno in a PGPROC that might be free.  This is safe
	 * because all PGPROCs' pgprocno fields are set during shared memory
	 * initialization and never change thereafter; so we will acquire the
	 * correct lock even if the leader PGPROC is in process of being recycled.
	 */
	leader_lwlock = LockHashPartitionLockByProc(leader);
	LWLockAcquire(leader_lwlock, LW_EXCLUSIVE);

	/* Is this the leader we're looking for? */
	if (leader->pid == pid && leader->lockGroupLeader == leader)
	{
		/* OK, join the group */
		ok = true;
		MyProc->lockGroupLeader = leader;
		dlist_push_tail(&leader->lockGroupMembers, &MyProc->lockGroupLink);
	}
	LWLockRelease(leader_lwlock);

	return ok;
}
