/*-------------------------------------------------------------------------
 *
 * pddiag.h -- production allocator/crash diagnostics, near-zero cost.
 *
 * Philosophy: observe, never interfere.  The event ring records sampled
 * alloc/free events with the real caller's return address (stored on entry
 * to the palloc/pfree funnels below, so the hot path pays one plain store,
 * and sampled events read it back) and the chunk's capacity (so a fault
 * anywhere inside a chunk range-matches its ALLOC event); the trace ring
 * records tagged events from any site (context lifecycle, ownership
 * handoffs, dispatcher teardowns); everything is analysed post-mortem by
 * the crash dump in pddiag.c, which runs inside GPDB's standard signal
 * handler.
 *
 * Always on; there is no configuration.  Overhead: ~1% CPU on alloc-dense
 * workloads (compile-time sampling: 1 of 2^k alloc/free operations, see
 * PD_ALLOC_MASK / PD_FREE_MASK below; trace entries are sampled too, see
 * PD_TRACE_MASK, because MemoryContextReset runs per scanned tuple in the
 * executor).  Memory: the event ring is ~39MB of BSS per process,
 * committed lazily page-by-page as events are recorded -- a long-lived
 * backend that has run enough allocations ends up with up to ~38MB of
 * private RSS.  The trace ring adds another 128KB.
 *
 * Coverage: the postmaster boots the diagnostics in PostmasterMain;
 * user backends (plus fts, startup, walreceiver, walsender) re-boot in
 * their own init paths.  Aux processes that run neither init path
 * (checkpointer, bgwriter, syslogger, stats collector, ...) inherit the
 * handler and the event hook through fork(), so they still record events
 * and dump their own pid-keyed report on a crash; they just never re-take
 * the signal slots if someone displaces the inherited handler.
 *
 * Coverage gaps (documented, not accidental): AllocSetRealloc of a large
 * (single-chunk block) allocation moves the block via gp_realloc() without
 * recording free+alloc events, so the ring history for the old address
 * ends at the last event before the move; AllocSetAllocHeader (shared
 * headers) is not instrumented either.  "ret" names the caller of the
 * palloc/pfree/MemoryContextAlloc funnel; code that invokes the context
 * methods pointer directly bypasses the funnels and attributes the event
 * to an earlier caller.
 *
 * Usage at a site of interest (one line, no new arrays/functions):
 *		pd_trace(PD_TAG_<something>, ptr, owner);
 *
 * Part of the standard build: always compiled in, always on.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PDDIAG_H
#define PDDIAG_H

#include "c.h"

/* trace tags (printed by name in the crash dump).  Adding a site anywhere
 * in the backend is one line: pd_trace(PD_TAG_<yours>, a, b); plus a name
 * entry in pddiag.c's pd_tag_names. */
#define PD_TAG_CTX_RESET		1	/* a=context */
#define PD_TAG_CTX_DELETE		2	/* a=context */

extern void pd_boot(void);
extern void pd_recheck(void);	/* retake displaced signal slots; idempotent */
extern void pd_event(int is_alloc, void *ptr, void *ctx, uint64 size, uint64 n);
extern void pd_trace(uint32 tag, void *a, void *b);
extern void pd_dump_crash(int signo, void *faultptr);

/* set by pd_boot; NULL only before boot or outside backend processes --
 * the common case costs a single load+branch on a hot static */
extern void (*pd_ev_hook) (int is_alloc, void *ptr, void *ctx, uint64 size, uint64 n);

/*
 * Real-caller capture.  palloc/pfree/MemoryContextAlloc* are the universal
 * funnels, so the return address stored at their entry is the code that
 * asked for the allocation -- exactly what the post-mortem report wants.
 * One plain store per funnel call; racy across threads, benign for
 * diagnostics (palloc from a non-main thread is a developer error anyway,
 * see aset.c).  pd_event reads it back only on recorded (1:2^k) events.
 */
extern void *volatile pd_last_ret;
#if defined(__GNUC__) || defined(__clang__)
#define PD_EV_ENTER() (pd_last_ret = __builtin_return_address(0))
#else
#define PD_EV_ENTER() ((void) 0)
#endif

#ifndef PD_ALLOC_MASK
#define PD_ALLOC_MASK 2047
#endif
#ifndef PD_FREE_MASK
#define PD_FREE_MASK 127
#endif
#ifndef PD_TRACE_MASK
#define PD_TRACE_MASK 63		/* record 1 of 64 trace events */
#endif

extern uint64 pd_diag_seq;

/* racy plain per-site counters: the sampling decision only, races benign */
#define PD_EV_ALLOC(p,c,z) do { \
		static uint64 pd_ctr_ = 0; \
		if (pd_ev_hook && ((++pd_ctr_ & PD_ALLOC_MASK) == 0)) \
			pd_ev_hook(1, p, c, z, __sync_fetch_and_add(&pd_diag_seq, 1)); \
	} while (0)
#define PD_EV_FREE(p,c,z) do { \
		static uint64 pd_ctr_ = 0; \
		if (pd_ev_hook && ((++pd_ctr_ & PD_FREE_MASK) == 0)) \
			pd_ev_hook(0, p, c, z, __sync_fetch_and_add(&pd_diag_seq, 1)); \
	} while (0)

#endif							/* PDDIAG_H */
