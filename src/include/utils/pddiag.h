/*-------------------------------------------------------------------------
 *
 * pddiag.h -- production allocator/crash diagnostics, near-zero cost.
 *
 * Philosophy: observe, never interfere.  The event ring records sampled
 * alloc/free events with the caller return address (captured only on
 * recorded events, so the common case pays nothing); the trace ring
 * records tagged events from any site (context lifecycle, ownership
 * handoffs, dispatcher teardowns); everything is analysed post-mortem by
 * the crash dump in pddiag.c, which runs inside GPDB's standard signal
 * handler.
 *
 * Always on; there is no configuration.  Overhead: ~1% on alloc-dense
 * workloads (compile-time sampling: 1 of 2^k alloc/free operations, see
 * PD_ALLOC_MASK / PD_FREE_MASK below).
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
extern void pd_event(int is_alloc, void *ptr, void *ctx, uint64 size, uint64 n);
extern void pd_trace(uint32 tag, void *a, void *b);
extern void pd_dump_crash(int signo, void *faultptr);

/* set by pd_boot when PD_DIAG is on; NULL in a disabled process */
extern void (*pd_ev_hook) (int is_alloc, void *ptr, void *ctx, uint64 size, uint64 n);

#ifndef PD_ALLOC_MASK
#define PD_ALLOC_MASK 2047
#endif
#ifndef PD_FREE_MASK
#define PD_FREE_MASK 127
#endif

extern uint64 pd_diag_seq;

/* racy plain per-site counters: the sampling decision only, races benign */
#define PD_EV_ALLOC(p,c) do { \
		static uint64 pd_ctr_ = 0; \
		if (pd_ev_hook && ((++pd_ctr_ & PD_ALLOC_MASK) == 0)) \
			pd_ev_hook(1, p, c, 0, __sync_fetch_and_add(&pd_diag_seq, 1)); \
	} while (0)
#define PD_EV_FREE(p,c,z) do { \
		static uint64 pd_ctr_ = 0; \
		if (pd_ev_hook && ((++pd_ctr_ & PD_FREE_MASK) == 0)) \
			pd_ev_hook(0, p, c, z, __sync_fetch_and_add(&pd_diag_seq, 1)); \
	} while (0)

#endif							/* PDDIAG_H */
