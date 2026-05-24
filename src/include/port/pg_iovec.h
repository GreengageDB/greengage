/*-------------------------------------------------------------------------
 *
 * pg_iovec.h
 *	  Header for vectored I/O functions, to use in place of <sys/uio.h>.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_iovec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_IOVEC_H
#define PG_IOVEC_H

#include <limits.h>

#include <sys/uio.h>

/*
 * If <limits.h> didn't define IOV_MAX, define our own.  POSIX requires at
 * least 16.
 */
#ifndef IOV_MAX
#define IOV_MAX 16
#endif

/* Define a reasonable maximum that is safe to use on the stack. */
#define PG_IOV_MAX Min(IOV_MAX, 32)

#ifdef HAVE_PREADV
#define pg_preadv preadv
#else
extern ssize_t pg_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset);
#endif

#ifdef HAVE_PWRITEV
#define pg_pwritev pwritev
#else
extern ssize_t pg_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset);
#endif

#endif							/* PG_IOVEC_H */
