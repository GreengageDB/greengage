/*-------------------------------------------------------------------------
 *
 * elog.h
 *	  POSTGRES error reporting/logging definitions.
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/elog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ELOG_H
#define ELOG_H

#include "c.h"
#include <sys/time.h>
#include <setjmp.h>

/* Error level codes */
#define DEBUG5		10			/* Debugging messages, in categories of
								 * decreasing detail. */
#define DEBUG4		11
#define DEBUG3		12
#define DEBUG2		13
#define DEBUG1		14			/* used by GUC debug_* variables */
#define LOG			15			/* Server operational messages; sent only to
								 * server log by default. */
#define LOG_SERVER_ONLY 16		/* Same as LOG for server reporting, but never
								 * sent to client. */
#define COMMERROR	LOG_SERVER_ONLY /* Client communication problems; same as
									 * LOG for server reporting, but never
									 * sent to client. */
#define INFO		17			/* Messages specifically requested by user (eg
								 * VACUUM VERBOSE output); always sent to
								 * client regardless of client_min_messages,
								 * but by default not sent to server log. */
#define NOTICE		18			/* Helpful messages to users about query
								 * operation; sent to client and not to server
								 * log by default. */
#define WARNING		19			/* Warnings.  NOTICE is for expected messages
								 * like implicit sequence creation by SERIAL.
								 * WARNING is for unexpected messages. */
#define ERROR		20			/* user error - abort transaction; return to
								 * known state */
/* Save ERROR value in PGERROR so it can be restored when Win32 includes
 * modify it.  We have to use a constant rather than ERROR because macros
 * are expanded only when referenced outside macros.
 */
#ifdef WIN32
#define PGERROR		20
#endif
#define FATAL		21			/* fatal error - abort process */
#define PANIC		22			/* take down the other backends with me */

 /* #define DEBUG DEBUG1 */	/* Backward compatibility with pre-7.3 */


/* macros for representing SQLSTATE strings compactly */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val) & 0x3F) + '0')

#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* These macros depend on the fact that '0' becomes a zero in SIXBIT */
#define ERRCODE_TO_CATEGORY(ec)  ((ec) & ((1 << 12) - 1))
#define ERRCODE_IS_CATEGORY(ec)  (((ec) & ~((1 << 12) - 1)) == 0)

/* SQLSTATE codes for errors are defined in a separate file */
#include "utils/errcodes.h"

/* Common error messages */
#define ERRMSG_GP_INSUFFICIENT_STATEMENT_MEMORY "insufficient memory reserved for statement"



/* threaded thing. 
 * Caller beware: ereport and elog can only be called from main thread.
 */
#ifdef WIN32
#include "pthread-win32.h"
extern DWORD main_tid;
#else
#include <pthread.h>
extern pthread_t main_tid;
#endif
#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#define mainthread() ((unsigned long) main_tid)
#else
#define mythread() ((unsigned long) pthread_self().p)
#define mainthread() ((unsigned long) main_tid.p)
#endif 

/*
 * Provide a way to prevent "errno" from being accidentally used inside an
 * elog() or ereport() invocation.  Since we know that some operating systems
 * define errno as something involving a function call, we'll put a local
 * variable of the same name as that function in the local scope to force a
 * compile error.  On platforms that don't define errno in that way, nothing
 * happens, so we get no warning ... but we can live with that as long as it
 * happens on some popular platforms.
 */
#if defined(errno) && defined(__linux__)
#define pg_prevent_errno_in_scope() int __errno_location pg_attribute_unused()
#elif defined(errno) && (defined(__darwin__) || defined(__freebsd__))
#define pg_prevent_errno_in_scope() int __error pg_attribute_unused()
#else
#define pg_prevent_errno_in_scope()
#endif


/*----------
 * New-style error reporting API: to be used in this way:
 *		ereport(ERROR,
 *				errcode(ERRCODE_UNDEFINED_CURSOR),
 *				errmsg("portal \"%s\" not found", stmt->portalname),
 *				... other errxxx() fields as needed ...);
 *
 * The error level is required, and so is a primary error message (errmsg
 * or errmsg_internal).  All else is optional.  errcode() defaults to
 * ERRCODE_INTERNAL_ERROR if elevel is ERROR or more, ERRCODE_WARNING
 * if elevel is WARNING, or ERRCODE_SUCCESSFUL_COMPLETION if elevel is
 * NOTICE or below.
 *
 * Before Postgres v12, extra parentheses were required around the
 * list of auxiliary function calls; that's now optional.
 *
 * ereport_domain() allows a message domain to be specified, for modules that
 * wish to use a different message catalog from the backend's.  To avoid having
 * one copy of the default text domain per .o file, we define it as NULL here
 * and have errstart insert the default text domain.  Modules can either use
 * ereport_domain() directly, or preferably they can override the TEXTDOMAIN
 * macro.
 *
 * If elevel >= ERROR, the call will not return; we try to inform the compiler
 * of that via pg_unreachable().  However, no useful optimization effect is
 * obtained unless the compiler sees elevel as a compile-time constant, else
 * we're just adding code bloat.  So, if __builtin_constant_p is available,
 * use that to cause the second if() to vanish completely for non-constant
 * cases.  We avoid using a local variable because it's not necessary and
 * prevents gcc from making the unreachability deduction at optlevel -O0.
 *----------
 */
#ifdef HAVE__BUILTIN_CONSTANT_P
#define ereport_domain(elevel, domain, ...)	\
	do { \
		pg_prevent_errno_in_scope(); \
		if (errstart(elevel, domain)) \
			__VA_ARGS__, errfinish(__FILE__, __LINE__, PG_FUNCNAME_MACRO); \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			pg_unreachable(); \
	} while(0)
#else							/* !HAVE__BUILTIN_CONSTANT_P */
#define ereport_domain(elevel, domain, ...)	\
	do { \
		const int elevel_ = (elevel); \
		pg_prevent_errno_in_scope(); \
		if (errstart(elevel_, domain)) \
			__VA_ARGS__, errfinish(__FILE__, __LINE__, PG_FUNCNAME_MACRO); \
		if (elevel_ >= ERROR) \
			pg_unreachable(); \
	} while(0)
#endif							/* HAVE__BUILTIN_CONSTANT_P */

#define ereport(elevel, ...)	\
	ereport_domain(elevel, TEXTDOMAIN, __VA_ARGS__)

#define TEXTDOMAIN NULL

/*
 * the error or log report is only issued if the predicate is true.
 */
#define ereportif(p, elevel, ...) \
	do { \
		if(p) ereport_domain(elevel, TEXTDOMAIN, __VA_ARGS__); \
	} while (0)

extern bool errstart(int elevel, const char *domain);
extern void errfinish(const char *filename, int lineno, const char *funcname);

extern void errcode(int sqlerrcode);

extern void errcode_for_file_access(void);
extern void errcode_for_socket_access(void);

extern int sqlstate_to_errcode(const char *sqlstate);
extern void errcode_to_sqlstate(int errcode, char outbuf[6]);

extern void errmsg(const char *fmt,...) pg_attribute_printf(1, 2);
extern void errmsg_internal(const char *fmt,...) pg_attribute_printf(1, 2);

extern void errmsg_plural(const char *fmt_singular, const char *fmt_plural,
						  unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern void errdetail(const char *fmt,...) pg_attribute_printf(1, 2);
extern void errdetail_internal(const char *fmt,...) pg_attribute_printf(1, 2);

extern void errdetail_log(const char *fmt,...) pg_attribute_printf(1, 2);

extern void errdetail_log_plural(const char *fmt_singular,
								 const char *fmt_plural,
								 unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern void errdetail_plural(const char *fmt_singular, const char *fmt_plural,
							 unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern void errhint(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * errcontext() is typically called in error context callback functions, not
 * within an ereport() invocation. The callback function can be in a different
 * module than the ereport() call, so the message domain passed in errstart()
 * is not usually the correct domain for translating the context message.
 * set_errcontext_domain() first sets the domain to be used, and
 * errcontext_msg() passes the actual message.
 */
#define errcontext	set_errcontext_domain(TEXTDOMAIN),	errcontext_msg

extern void set_errcontext_domain(const char *domain);

extern void errcontext_msg(const char *fmt,...) pg_attribute_printf(1, 2);

extern void errhidestmt(bool hide_stmt);
extern void errhidecontext(bool hide_ctx);

extern int	errprintstack(bool printstack);

extern void errfunction(const char *funcname);
extern void errposition(int cursorpos);

extern void internalerrposition(int cursorpos);
extern void internalerrquery(const char *query);

extern void err_generic_string(int field, const char *str);

extern int	geterrcode(void);
extern int	geterrposition(void);
extern int	getinternalerrposition(void);

extern int errFatalReturn(bool fatalReturn); /* GPDB: true => return on FATAL error */

/*----------
 * Old-style error reporting API: to be used in this way:
 *		elog(ERROR, "portal \"%s\" not found", stmt->portalname);
 *----------
 */
#define elog(elevel, ...)  \
	ereport(elevel, errmsg_internal(__VA_ARGS__))


/* Support for constructing error strings separately from ereport() calls */

extern void pre_format_elog_string(int errnumber, const char *domain);
extern char *format_elog_string(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * The message is only logged if a predicate is true.
 * This is a replacement for the common pattern of
 *
 * if (guc)
 *     elog(LOG, ...)
 */
#define elogif(p, ...) do { \
	if (p) elog(__VA_ARGS__); \
    } while(false);

/* Support for attaching context information to error reports */

typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

extern PGDLLIMPORT ErrorContextCallback *error_context_stack;


/*----------
 * API for catching ereport(ERROR) exits.  Use these macros like so:
 *
 *		PG_TRY();
 *		{
 *			... code that might throw ereport(ERROR) ...
 *		}
 *		PG_CATCH();
 *		{
 *			... error recovery code ...
 *		}
 *		PG_END_TRY();
 *
 * (The braces are not actually necessary, but are recommended so that
 * pgindent will indent the construct nicely.)  The error recovery code
 * can either do PG_RE_THROW to propagate the error outwards, or do a
 * (sub)transaction abort. Failure to do so may leave the system in an
 * inconsistent state for further processing.
 *
 * Note: while the system will correctly propagate any new ereport(ERROR)
 * occurring in the recovery section, there is a small limit on the number
 * of levels this will work for.  It's best to keep the error recovery
 * section simple enough that it can't generate any new errors, at least
 * not before popping the error stack.
 *
 * Note: an ereport(FATAL) will not be caught by this construct; control will
 * exit straight through proc_exit().  Therefore, do NOT put any cleanup
 * of non-process-local resources into the error recovery section, at least
 * not without taking thought for what will happen during ereport(FATAL).
 * The PG_ENSURE_ERROR_CLEANUP macros provided by storage/ipc.h may be
 * helpful in such cases.
 *
 * Note: if a local variable of the function containing PG_TRY is modified
 * in the PG_TRY section and used in the PG_CATCH section, that variable
 * must be declared "volatile" for POSIX compliance.  This is not mere
 * pedantry; we have seen bugs from compilers improperly optimizing code
 * away when such a variable was not marked.  Beware that gcc's -Wclobbered
 * warnings are just about entirely useless for catching such oversights.
 *----------
 */
#define PG_TRY()  \
	do { \
		sigjmp_buf *save_exception_stack = PG_exception_stack; \
		ErrorContextCallback *save_context_stack = error_context_stack; \
		sigjmp_buf local_sigjmp_buf; \
		if (sigsetjmp(local_sigjmp_buf, 0) == 0) \
		{ \
			PG_exception_stack = &local_sigjmp_buf

#define PG_CATCH()	\
		} \
		else \
		{ \
			PG_exception_stack = save_exception_stack; \
			error_context_stack = save_context_stack

#define PG_END_TRY()  \
		} \
		PG_exception_stack = save_exception_stack; \
		error_context_stack = save_context_stack; \
	} while (0)

/*
 * Some compilers understand pg_attribute_noreturn(); for other compilers,
 * insert pg_unreachable() so that the compiler gets the point.
 */
#ifdef HAVE_PG_ATTRIBUTE_NORETURN
#define PG_RE_THROW()  \
	pg_re_throw()
#else
#define PG_RE_THROW()  \
	(pg_re_throw(), pg_unreachable())
#endif

extern PGDLLIMPORT sigjmp_buf *PG_exception_stack;


/* Stuff that error handlers might want to use */

/*
 * ErrorData holds the data accumulated during any one ereport() cycle.
 * Any non-NULL pointers must point to palloc'd data.
 * (The const pointers are an exception; we assume they point at non-freeable
 * constant strings.)
 */
typedef struct ErrorData
{
	int			elevel;			/* error level */
	bool		output_to_server;	/* will report to server log? */
	bool		output_to_client;	/* will report to client? */
	bool		show_funcname;	/* true to force funcname inclusion */
    bool        omit_location;  /* GPDB: don't add filename:line# and stack trace */
    bool        fatal_return;   /* GPDB: true => return instead of proc_exit() */
	bool		hide_stmt;		/* true to prevent STATEMENT: inclusion */
	bool		hide_ctx;		/* true to prevent CONTEXT: inclusion */
	const char *filename;		/* __FILE__ of ereport() call */
	int			lineno;			/* __LINE__ of ereport() call */
	const char *funcname;		/* __func__ of ereport() call */
	const char *domain;			/* message domain */
	const char *context_domain; /* message domain for context message */
	int			sqlerrcode;		/* encoded ERRSTATE */
	char	   *message;		/* primary error message (translated) */
	char	   *detail;			/* detail error message */
	char	   *detail_log;		/* detail error message for server log only */
	char	   *hint;			/* hint message */
	char	   *context;		/* context message */
	const char *message_id;		/* primary message's id (original string) */
	char	   *schema_name;	/* name of schema */
	char	   *table_name;		/* name of table */
	char	   *column_name;	/* name of column */
	char	   *datatype_name;	/* name of datatype */
	char	   *constraint_name;	/* name of constraint */
	int			cursorpos;		/* cursor index into query string */
	int			internalpos;	/* cursor index into internalquery */
	char	   *internalquery;	/* text of internally-generated query */
	int			saved_errno;	/* errno at entry */

	void	   *stacktracearray[30];
	size_t		stacktracesize;
	bool		printstack;		/* force output stack trace */

	/* context containing associated non-constant strings */
	struct MemoryContextData *assoc_context;
} ErrorData;

extern void EmitErrorReport(void);
extern ErrorData *CopyErrorData(void);
extern void FreeErrorData(ErrorData *edata);
extern void FlushErrorState(void);
extern void ReThrowError(ErrorData *edata) pg_attribute_noreturn();
extern void ThrowErrorData(ErrorData *edata);
extern void pg_re_throw(void) pg_attribute_noreturn();

extern char *GetErrorContextStack(void);

/* Hook for intercepting messages before they are sent to the server log */
typedef void (*emit_log_hook_type) (ErrorData *edata);
extern PGDLLIMPORT emit_log_hook_type emit_log_hook;

extern ErrorData *errfinish_and_return(const char *filename, int lineno, const char *funcname);

/*
 * GPDB: elog_exception_statement
 * Write statement in log file if an exception was encountered during
 * its execution.
 */
extern void	elog_exception_statement(const char* statement);

/*
 * CDB: elog_demote
 *
 * A PG_CATCH() handler can call this to downgrade the error that it is
 * currently handling to a level lower than ERROR.  The caller should
 * then do PG_RE_THROW() to proceed to the next error handler.  
 *
 * Clients using libpq cannot receive normal output together with an error.
 * The libpq frontend discards any results already buffered when a command
 * completes with an error notification of level ERROR or higher.  
 *
 * elog_demote() can be used to reduce the error level reported to the client
 * so that libpq won't suppress normal output, while the backend still frees
 * resources, aborts the transaction, etc, as usual.
 *
 * Returns true if successful, false if the request is disallowed.
 */
bool        elog_demote(int downgrade_to_elevel);

/*
 * CDB: elog_dismiss 
 *
 * A PG_CATCH() handler can call this to downgrade the error that it is
 * currently handling to a level lower than ERROR, report it to the log
 * and/or client as appropriate, and purge it from the error system.
 *
 * This shouldn't be attempted unless the caller is certain that the
 * error does not need the services of upper level error handlers to
 * release resources, abort the transaction, etc.
 *
 * Returns true if successful, in which case the error has been expunged
 * and the caller should not do PG_RE_THROW(), but should instead fall or
 * jump out of the PG_CATCH() handler and resume normal execution.
 *
 * Returns false if unsuccessful; then the caller should carry on as 
 * PG_CATCH() handlers ordinarily do, and exit via PG_RE_THROW().  
 */
bool        elog_dismiss(int downgrade_to_elevel);   

/*
 * CDB: elog_geterrcode
 * Return the SQLSTATE code for the error currently being handled, or 0.
 *
 * This is only intended for use in error handlers.
 */
int         elog_geterrcode(void);      
int         elog_getelevel(void);      
char        *elog_message(void);


/* GUC-configurable parameters */

typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
}			PGErrorVerbosity;

extern int	Log_error_verbosity;
extern char *Log_line_prefix;
extern int	Log_destination;
extern char *Log_destination_string;
extern bool syslog_sequence_numbers;
extern bool syslog_split_messages;

/* Log destination bitmap */
#define LOG_DESTINATION_STDERR	 1
#define LOG_DESTINATION_SYSLOG	 2
#define LOG_DESTINATION_EVENTLOG 4
#define LOG_DESTINATION_CSVLOG	 8

/* Other exported functions */
extern void DebugFileOpen(void);
extern char *unpack_sql_state(int sql_state);
extern bool in_error_recursion_trouble(void);

#ifdef HAVE_SYSLOG
extern void set_syslog_parameters(const char *ident, int facility);
#endif

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
extern void write_stderr(const char *fmt,...) pg_attribute_printf(1, 2);

extern void write_message_to_server_log(int elevel,
										int sqlerrcode,
										const char *message,
										const char *detail,
										const char *hint,
										const char *query_text,
										int cursorpos,
										int internalpos,
										const char *internalquery,
										const char *context,
										const char *funcname,
										bool show_funcname,
										const char *filename,
										int lineno,
										int stacktracesize,
										bool omit_location,
										void* const *stacktracearray,
										bool printstack);

extern void debug_backtrace(void);
extern uint32 gp_backtrace(void **stackAddresses, uint32 maxStackDepth);
extern char *gp_stacktrace(void **stackAddresses, uint32 stackDepth);

/* stack base pointer, defined in postgres.c */
extern char *stack_base_ptr;

/* GUCs */
extern bool gp_log_stack_trace_lines;   /* session GUC, controls line info in stack traces */

extern const char *SegvBusIllName(int signal);
extern void InitStandardHandlerForSigillSigsegvSigbus_OnMainThread(void);
extern void StandardHandlerForSigillSigsegvSigbus_OnMainThread(char * processName, SIGNAL_ARGS);

#endif							/* ELOG_H */
