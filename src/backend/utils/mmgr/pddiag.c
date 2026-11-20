
/*-------------------------------------------------------------------------
 *
 * pddiag.c -- allocator diagnostics, always on, near-zero runtime cost.
 *
 * Philosophy: observe, never interfere.  No behavior changes to the
 * allocator; events are append-only records in a static ring buffer; the
 * stamp is write-only; all analysis happens at crash time.
 *
 * Configuration: none.  Sampling is compile-time (PD_ALLOC_MASK /
 * PD_FREE_MASK in pddiag.h: one event per 2^k alloc/free operations).
 *
 * Event ring entry: seq, ptr, ctx, size, caller return address, tid,
 * flags.  The crash handler (async-signal-safe: open/write + custom u64
 * formatting, one write() per fully formatted line) appends
 * <logdir>/pddiag-<pid>.log with: fault address, chunk-header probe, the
 * ring history for the fault pointer (or the newest events), and the
 * tagged trace log; then the previous handler runs (GPDB's segv printer).
 *
 * The ring and the trace log are static buffers, so they need no setup,
 * cannot fail to allocate, and are directly inspectable in core dumps.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/pddiag.h"
#include "miscadmin.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

/* keep this TU free of elog/palloc: it must run in a signal handler */

typedef struct PdEvent
{
	uint64		seq;
	void	   *ptr;
	void	   *ctx;
	uint64		size;
	void	   *ret;			/* return address of the alloc/free caller */
	uint64		tid;
	uint32		flags;			/* bit0 alloc, bit1 free */
	uint32		pad;
} PdEvent;

/*
 * Compile-time policy: always on, no configuration.  Sampling is a power
 * of two minus one (record one event per 2^k operations).  The ring is a
 * static buffer: BSS pages commit lazily like a NORESERVE mmap, there is
 * no failure mode, and the symbol is directly visible in core dumps.
 */
#ifndef PD_RING_ENTRIES
#define PD_RING_ENTRIES ((uint64)700000)	/* ~39MB of PdEvent (56 bytes each) */
#endif
#define PD_MLOG_ENTRIES 4096

typedef struct PdMLogEnt
{
	uint64		seq;
	void	   *a;
	void	   *b;
	uint64		tag;
} PdMLogEnt;

static PdEvent pd_ring_buf[PD_RING_ENTRIES];
uint64 pd_diag_seq = 0;		/* exported for the caller-side pre-check */
static struct sigaction pd_old_segv, pd_old_abrt, pd_old_bus;

static PdMLogEnt pd_trace_ring[PD_MLOG_ENTRIES];
static uint64 pd_trace_idx = 0;
static uint64 pd_fault_addr = 0;	/* captured by the SA_SIGINFO handler */

static const struct
{
	uint32		tag;
	const char *name;
} pd_tag_names[] =
{
	{PD_TAG_CTX_RESET, "CTX_RESET"},
	{PD_TAG_CTX_DELETE, "CTX_DELETE"},
};

/*
 * Compile-time policy: always on, no configuration.  Sampling is a power
 * of two minus one (record one event per 2^k operations).
 */
#ifndef PD_ALLOC_MASK
#define PD_ALLOC_MASK	2047	/* record 1 of 2048 allocs */
#endif
#ifndef PD_FREE_MASK
#define PD_FREE_MASK	127	/* record 1 of 128 frees */
#endif

static uint64
pd_tid(void)
{
	static __thread uint64 pd_tls_tid = 0;

	/* one syscall per thread; the event path then pays a load+branch */
	if (pd_tls_tid == 0)
		pd_tls_tid = (uint64) syscall(SYS_gettid);
	return pd_tls_tid;
}

static void pd_install(void); /* forward */
static void pd_recheck(void);

/* hook pointer: NULL in shipped builds -> the per-op cost is a single
 * load+branch on a hot static; pd_boot points it at pd_event. */
void		(*pd_ev_hook) (int is_alloc, void *ptr, void *ctx, uint64 size, uint64 n) = NULL;

void
pd_event(int is_alloc, void *ptr, void *ctx, uint64 size, uint64 n)
{
	PdEvent    *e;

	if ((n & 0xFFF) == 0)
		pd_recheck();
	e = &pd_ring_buf[n % PD_RING_ENTRIES];
	e->seq = n;
	e->ptr = ptr;
	e->ctx = ctx;
	e->size = size;
	e->ret = __builtin_return_address(0);
	e->tid = pd_tid();
	e->flags = is_alloc ? 1 : 2;
	if (!is_alloc && ptr)
		*(volatile uint64 *) ptr = 0x706466726565ULL;	/* stamp on recorded frees */
}

/* ownership micro-logs: fixed arrays, overwrite-oldest, no allocation */
static void
pd_mlog(PdMLogEnt * log, uint64 * idx, void *a, void *b, uint64 tag)
{
	uint64		n = __sync_fetch_and_add(idx, 1) % PD_MLOG_ENTRIES;

	log[n].seq = pd_diag_seq;
	log[n].a = a;
	log[n].b = b;
	log[n].tag = tag;
}

/* generic tagged trace: one line at any site of interest, no per-site
 * arrays or functions */
void
pd_trace(uint32 tag, void *a, void *b)
{
	pd_mlog(pd_trace_ring, &pd_trace_idx, (void *) (uintptr_t) tag, a, (uint64) (uintptr_t) b);
}

/* ---------- crash-time mini-report (async-signal-safe) ---------- */

static int
pd_u64(char *out, uint64 v)
{
	char		tmp[24];
	int			i = 0, n = 0;

	if (!v)
	{
		out[0] = '0';
		return 1;
	}
	while (v)
	{
		tmp[i++] = '0' + (v % 10);
		v /= 10;
	}
	while (i)
		out[n++] = tmp[--i];
	return n;
}

static int
pd_hex(char *out, uint64 v)
{
	static const char h[] = "0123456789abcdef";
	int			shift;
	int			n = 0;
	int			lead = 1;

	for (shift = 60; shift >= 0; shift -= 4)
	{
		int			d = (v >> shift) & 0xF;

		if (lead && d == 0 && shift)
			continue;
		lead = 0;
		out[n++] = h[d];
	}
	if (n == 0)
		out[n++] = '0';
	return n;
}

/* one write() per fully formatted line: a partial line is far worse
 * than a lost one, and shared state is forbidden here (signal context,
 * other threads) */
static void
pd_emit(int fd, const char *s, int len)
{
	while (len > 0)
	{
		ssize_t		w = write(fd, s, len);

		if (w <= 0)
			return;
		s += w;
		len -= (int) w;
	}
}

static void
pd_emit_str(int fd, const char *label, const char *val)
{
	char		buf[256];
	int			n = 0;

	while (*label && n < 200)
		buf[n++] = *label++;
	while (*val && n < 240)
		buf[n++] = *val++;
	buf[n++] = '\n';
	pd_emit(fd, buf, n);
}

static void
pd_emit_u64(int fd, const char *label, uint64 v)
{
	char		buf[32];
	int			n = pd_u64(buf, v);

	buf[n] = '\0';
	pd_emit_str(fd, label, buf);
}

static void
pd_emit_hex(int fd, const char *label, uint64 v)
{
	char		buf[32];
	int			n = pd_hex(buf, v);

	buf[n] = '\0';
	pd_emit_str(fd, label, buf);
}

/* ring history for one pointer: newest-first, bounded scan */
static void
pd_dump_ring_for(int fd, void *ptr)
{
	uint64		seq = pd_diag_seq;
	uint64		i;
	int			found = 0;
	char		buf[160];
	int			n;

	if (ptr == NULL)
	{
		pd_emit_str(fd, "", "== ring tail (newest 64, unfiltered) ==");
		for (i = 0; i < PD_RING_ENTRIES && i < seq && (int) i < 64; i++)
		{
			PdEvent    *e = &pd_ring_buf[(seq - 1 - i) % PD_RING_ENTRIES];
			char		buf[160];
			int			n = 0;

			if (e->seq == 0 || e->seq > seq || e->seq + PD_RING_ENTRIES < seq)
				continue;		/* never written or stale (wrapped) slot */

			memcpy(buf, "  ev#", 5);
			n = 5 + pd_u64(buf + 5, e->seq);
			/* NB: both labels are 11 bytes (" FREE  ptr=" carries one extra
			 * space to align with " ALLOC ptr="); copying a fixed 12 would
			 * emit the literal's NUL terminator */
			if (e->flags & 1)
			{
				memcpy(buf + n, " ALLOC ptr=", 11);
				n += 11;
			}
			else
			{
				memcpy(buf + n, " FREE  ptr=", 11);
				n += 11;
			}
			n += pd_hex(buf + n, (uint64) (uintptr_t) e->ptr);
			memcpy(buf + n, " ctx=", 5);
			n += 5;
			n += pd_hex(buf + n, (uint64) (uintptr_t) e->ctx);
			memcpy(buf + n, " size=", 6);
			n += 6;
			n += pd_u64(buf + n, e->size);
			memcpy(buf + n, " ret=", 5);
			n += 5;
			n += pd_hex(buf + n, (uint64) (uintptr_t) e->ret);
			buf[n++] = '\n';
			pd_emit(fd, buf, n);
		}
		return;
	}
	pd_emit_str(fd, "", "== ring history for fault ptr (newest first, max 40) ==");
	for (i = 0; i < PD_RING_ENTRIES && i < seq && found < 40; i++)
	{
		PdEvent    *e = &pd_ring_buf[(seq - 1 - i) % PD_RING_ENTRIES];
		uint64		addr = (uint64) (uintptr_t) ptr;
		uint64		eptr = (uint64) (uintptr_t) e->ptr;

		/* exact ptr match, or an ALLOC event whose recorded chunk covers
		 * the fault address (fault landed inside a chunk, not at its base) */
		if (e->ptr != ptr &&
			!((e->flags & 1) && e->size > 0 &&
			  addr >= eptr && addr < eptr + e->size))
			continue;
		n = 0;
		memcpy(buf, "  ev#", 5);
		n = 5 + pd_u64(buf + 5, e->seq);
		/* NB: both labels are 11 bytes, see the tail dump above */
		if (e->flags & 1)
		{
			memcpy(buf + n, " ALLOC ptr=", 11);
			n += 11;
		}
		else
		{
			memcpy(buf + n, " FREE  ptr=", 11);
			n += 11;
		}
		n += pd_hex(buf + n, (uint64) (uintptr_t) e->ptr);
		memcpy(buf + n, " ctx=", 5);
		n += 5;
		n += pd_hex(buf + n, (uint64) (uintptr_t) e->ctx);
		memcpy(buf + n, " size=", 6);
		n += 6;
		n += pd_u64(buf + n, e->size);
		buf[n++] = '\n';
		pd_emit(fd, buf, n);
		found++;
	}
	if (!found)
		pd_emit_str(fd, "", "  (no ring entries for ptr)");
}

static const char *
pd_tag_name(uint64 tag)
{
	uint32		i;

	for (i = 0; i < sizeof(pd_tag_names) / sizeof(pd_tag_names[0]); i++)
		if (pd_tag_names[i].tag == tag)
			return pd_tag_names[i].name;
	return "?";
}

static void
pd_dump_trace(int fd)
{
	uint64		i,
				cnt,
				idx = pd_trace_idx;
	char		buf[200];
	int			n;

	pd_emit_str(fd, "", "== trace log (newest 32) ==");
	cnt = idx < PD_MLOG_ENTRIES ? idx : PD_MLOG_ENTRIES;
	for (i = 0; i < cnt && i < 32; i++)
	{
		PdMLogEnt  *e = &pd_trace_ring[(idx - 1 - i) % PD_MLOG_ENTRIES];
		const char *tn;

		if (e->seq == 0)
			continue;
		tn = pd_tag_name((uint64) (uintptr_t) e->a);
		n = 0;
		memcpy(buf, "  #", 3);
		n = 3 + pd_u64(buf + 3, e->seq);
		buf[n++] = ' ';
		while (*tn && n < 60)
			buf[n++] = *tn++;
		memcpy(buf + n, " a1=", 4);
		n += 4;
		n += pd_hex(buf + n, (uint64) (uintptr_t) e->b);
		memcpy(buf + n, " a2=", 4);
		n += 4;
		n += pd_hex(buf + n, e->tag);
		buf[n++] = '\n';
		pd_emit(fd, buf, n);
	}
}

static void pd_dump_trace(int fd);
static volatile int pd_dumping = 0;

/* async-safe replacement for snprintf("%s%s%d.log"): only strlen-width
 * string copies and the custom u64 formatter */
static int
pd_open_log(const char *suffix, int pid)
{
	char		path[640];
	const char *dir = DataDir;
	int			n = 0;

	while (dir[n] && n < (int) sizeof(path) - 48)
	{
		path[n] = dir[n];
		n++;
	}
	while (*suffix && n < (int) sizeof(path) - 32)
		path[n++] = *suffix++;
	n += pd_u64(path + n, (uint64) pid);
	memcpy(path + n, ".log", 5);
	n += 5;
	return open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
}

static void
pd_dump_crash_impl(int signo, uint64 fault)
{
	int			fd = -1;

	if (pd_dumping)
		return;
	pd_dumping = 1;
	/* next to the server logs: try the usual log directories, fall back to
	 * the data directory; O_APPEND keeps concurrent crashes separated */
	fd = pd_open_log("/log/pddiag-", (int) getpid());
	if (fd < 0)
		fd = pd_open_log("/pg_log/pddiag-", (int) getpid());
	if (fd < 0)
		fd = pd_open_log("/pddiag-", (int) getpid());
	if (fd < 0)
		return;
	pd_emit_str(fd, "", "pddiag crash report");
	pd_emit_u64(fd, "signo=", (uint64) signo);
	pd_emit_hex(fd, "fault=", fault);
	pd_emit_u64(fd, "seq=", pd_diag_seq);
	/* chunk-header probe: 16 bytes before payload; sanity-bounded, may
	 * itself fault (we are dying anyway) */
	if (fault > 64)
	{
		uint64		hdr0 = *(volatile uint64 *) (uintptr_t) (fault - 16);
		uint64		hdr1 = *(volatile uint64 *) (uintptr_t) (fault - 8);

		pd_emit_hex(fd, "chunk_hdr0=", hdr0);
		pd_emit_hex(fd, "chunk_hdr1=", hdr1);
		if (hdr1 > 0 && hdr1 < (1ULL << 40))
			pd_emit_u64(fd, "chunk_size_guess=", hdr1);
	}
	if (fault == 0 && pd_fault_addr != 0)
		fault = pd_fault_addr;
	pd_dump_ring_for(fd, (void *) (uintptr_t) fault);
	pd_dump_trace(fd);
	pd_emit_str(fd, "", "end.");
	close(fd);
}

/* public entry: called from our SA_SIGINFO handler (fault known) and from
 * GPDB's StandardHandlerForSigillSigsegvSigbus_OnMainThread (fault unknown) */
void
pd_dump_crash(int signo, void *faultptr)
{
	pd_dump_crash_impl(signo, (uint64) (uintptr_t) faultptr);
}

/* chain to the previous handler.  GPDB installs its crash handlers via
 * pqsignal(), i.e. through sa_handler WITHOUT SA_SIGINFO, so the union
 * member used for the call must match how the old action was registered:
 * calling a 1-arg handler through sa_sigaction only works by ABI accident.
 * SIG_DFL: re-raise for a core; SIG_IGN: swallow. */
static void
pd_chain(int signo, siginfo_t *si, void *uc, const struct sigaction *old)
{
	if (old->sa_flags & SA_SIGINFO)
		old->sa_sigaction(signo, si, uc);
	else if (old->sa_handler == SIG_IGN && signo != SIGSEGV && signo != SIGBUS)
		return;			/* honor SIG_IGN (only meaningful for SIGABRT) */
	else if (old->sa_handler == SIG_IGN || old->sa_handler == SIG_DFL)
	{
		/* no previous handler, or a synchronous fault that cannot be
		 * meaningfully ignored: die by the default action, with a core */
		signal(signo, SIG_DFL);
		raise(signo);
	}
	else
		old->sa_handler(signo);
}

static void
pd_handler(int signo, siginfo_t *si, void *uc)
{
	/* si_addr is only meaningful for a synchronous hardware fault
	 * (SEGV_MAPERR/ACCERR/...: 0 < si_code < 0x80).  For SI_USER/SI_QUEUE
	 * (kill, raise, timer) the same union slot holds pid/uid or timer data,
	 * and SI_KERNEL (0x80) has no address either. */
	if (si && si->si_addr &&
		si->si_code > 0 && si->si_code < 0x80)
		pd_fault_addr = (uint64) (uintptr_t) si->si_addr;
	pd_dump_crash_impl(signo, pd_fault_addr);
	/* chain to the previous handler (GPDB prints its stack) */
	if (signo == SIGSEGV)
		pd_chain(signo, si, uc, &pd_old_segv);
	else if (signo == SIGABRT)
		pd_chain(signo, si, uc, &pd_old_abrt);
	else
		pd_chain(signo, si, uc, &pd_old_bus);
}

static void
pd_recheck(void)
{
	struct sigaction cur;

	sigaction(SIGSEGV, NULL, &cur);
	if (cur.sa_sigaction != pd_handler)
		pd_install();
}

/* called from InitStandardHandlerForSigillSigsegvSigbus_OnMainThread (every
 * postmaster/backend start) so the ring and the signal handler exist before
 * any query work allocates */
void
pd_boot(void)
{
	pd_install();
	pd_ev_hook = pd_event;
}

/*
 * (Re)install our handler on top of the current one, snapshotting the
 * current action as the chain target -- but never snapshot our own handler
 * (that would chain to ourselves forever).  Idempotent and safe to call
 * concurrently: sigaction installs and returns the displaced action
 * atomically, and pd_recheck() relies on this to take the SIGSEGV slot
 * back after GPDB's pqsignal() overwrites us at process start.
 */
static void
pd_install(void)
{
	struct sigaction sa,
				cur;

	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = pd_handler;
	sa.sa_flags = SA_SIGINFO | SA_NODEFER;
	sigemptyset(&sa.sa_mask);

	if (sigaction(SIGSEGV, &sa, &cur) == 0 &&
		cur.sa_sigaction != pd_handler)
		pd_old_segv = cur;
	if (sigaction(SIGABRT, &sa, &cur) == 0 &&
		cur.sa_sigaction != pd_handler)
		pd_old_abrt = cur;
	if (sigaction(SIGBUS, &sa, &cur) == 0 &&
		cur.sa_sigaction != pd_handler)
		pd_old_bus = cur;
}
