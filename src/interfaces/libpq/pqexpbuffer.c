/*-------------------------------------------------------------------------
 *
 * pqexpbuffer.c
 *
 * PQExpBuffer provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with malloc().
 *
 * This module is essentially the same as the backend's StringInfo data type,
 * but it is intended for use in frontend libpq and client applications.
 * Thus, it does not rely on palloc() nor elog(), nor psprintf.c which
 * will exit() on error.
 *
 * It does rely on vsnprintf(); if configure finds that libc doesn't provide
 * a usable vsnprintf(), then a copy of our own implementation of it will
 * be linked into libpq.
 *
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/interfaces/libpq/pqexpbuffer.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * This file is compiled with both frontend and backend codes, symlinked by
 * src/backend/Makefile, and use macro FRONTEND to switch.
 *
 * Include "c.h" to adopt Greengage C types. Don't include "postgres_fe.h",
 * which only defines FRONTEND besides including "c.h"
 */
#include "c.h"

#include <limits.h>

#include "pqexpbuffer.h"

#ifdef WIN32
#include "win32.h"
#endif


/* All "broken" PQExpBuffers point to this string. */
static const char oom_buffer[1] = "";


/*
 * markPQExpBufferBroken
 *
 * Put a PQExpBuffer in "broken" state if it isn't already.
 */
static void
markPQExpBufferBroken(PQExpBuffer str)
{
	if (str->data != oom_buffer)
		free(str->data);

	/*
	 * Casting away const here is a bit ugly, but it seems preferable to not
	 * marking oom_buffer const.  We want to do that to encourage the compiler
	 * to put oom_buffer in read-only storage, so that anyone who tries to
	 * scribble on a broken PQExpBuffer will get a failure.
	 */
	str->data = (char *) oom_buffer;
	str->len = 0;
	str->maxlen = 0;
}

/*
 * createPQExpBuffer
 *
 * Create an empty 'PQExpBufferData' & return a pointer to it.
 */
PQExpBuffer
createPQExpBuffer(void)
{
	PQExpBuffer res;

	res = (PQExpBuffer) malloc(sizeof(PQExpBufferData));
	if (res != NULL)
		initPQExpBuffer(res);

	return res;
}

/*
 * initPQExpBuffer
 *
 * Initialize a PQExpBufferData struct (with previously undefined contents)
 * to describe an empty string.
 */
void
initPQExpBuffer(PQExpBuffer str)
{
	str->data = (char *) malloc(INITIAL_EXPBUFFER_SIZE);
	if (str->data == NULL)
	{
		str->data = (char *) oom_buffer;		/* see comment above */
		str->maxlen = 0;
		str->len = 0;
	}
	else
	{
		str->maxlen = INITIAL_EXPBUFFER_SIZE;
		str->len = 0;
		str->data[0] = '\0';
	}
}

/*
 * destroyPQExpBuffer(str);
 *
 *		free()s both the data buffer and the PQExpBufferData.
 *		This is the inverse of createPQExpBuffer().
 */
void
destroyPQExpBuffer(PQExpBuffer str)
{
	if (str)
	{
		termPQExpBuffer(str);
		free(str);
	}
}

/*
 * termPQExpBuffer(str)
 *		free()s the data buffer but not the PQExpBufferData itself.
 *		This is the inverse of initPQExpBuffer().
 */
void
termPQExpBuffer(PQExpBuffer str)
{
	if (str->data != oom_buffer)
		free(str->data);
	/* just for luck, make the buffer validly empty. */
	str->data = (char *) oom_buffer;	/* see comment above */
	str->maxlen = 0;
	str->len = 0;
}

/*
 * resetPQExpBuffer
 *		Reset a PQExpBuffer to empty
 *
 * Note: if possible, a "broken" PQExpBuffer is returned to normal.
 */
void
resetPQExpBuffer(PQExpBuffer str)
{
	if (str)
	{
		if (str->data != oom_buffer)
		{
			str->len = 0;
			str->data[0] = '\0';
		}
		else
		{
			/* try to reinitialize to valid state */
			initPQExpBuffer(str);
		}
	}
}

/*
 * enlargePQExpBuffer
 * Make sure there is enough space for 'needed' more bytes in the buffer
 * ('needed' does not include the terminating null).
 *
 * Returns 1 if OK, 0 if failed to enlarge buffer.  (In the latter case
 * the buffer is left in "broken" state.)
 */
int
enlargePQExpBuffer(PQExpBuffer str, size_t needed)
{
	size_t		newlen;
	char	   *newdata;

	if (PQExpBufferBroken(str))
		return 0;				/* already failed */

	/*
	 * Guard against ridiculous "needed" values, which can occur if we're fed
	 * bogus data.  Without this, we can get an overflow or infinite loop in
	 * the following.
	 */
	if (needed >= ((size_t) INT_MAX - str->len))
	{
		markPQExpBufferBroken(str);
		return 0;
	}

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= INT_MAX */

	if (needed <= str->maxlen)
		return 1;				/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = (str->maxlen > 0) ? (2 * str->maxlen) : 64;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to INT_MAX in case we went past it.  Note we are assuming here
	 * that INT_MAX <= UINT_MAX/2, else the above loop could overflow.  We
	 * will still have newlen >= needed.
	 */
	if (newlen > (size_t) INT_MAX)
		newlen = (size_t) INT_MAX;

	newdata = (char *) realloc(str->data, newlen);
	if (newdata != NULL)
	{
		str->data = newdata;
		str->maxlen = newlen;
		return 1;
	}

	markPQExpBufferBroken(str);
	return 0;
}

/*
 * printfPQExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and insert it into str.  More space is allocated to str if necessary.
 * This is a convenience routine that does the same thing as
 * resetPQExpBuffer() followed by appendPQExpBuffer().
 */
void
printfPQExpBuffer(PQExpBuffer str, const char *fmt,...)
{
	va_list		args;
	bool		done;

	resetPQExpBuffer(str);

	if (PQExpBufferBroken(str))
		return;					/* already failed */

	/* Loop in case we have to retry after enlarging the buffer. */
	do
	{
		va_start(args, fmt);
		done = appendPQExpBufferVA(str, fmt, args);
		va_end(args);
	} while (!done);
}

/*
 * appendPQExpBuffer
 *
 * Format text data under the control of fmt (an sprintf-like format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void
appendPQExpBuffer(PQExpBuffer str, const char *fmt,...)
{
	va_list		args;
	bool		done;

	if (PQExpBufferBroken(str))
		return;					/* already failed */

	/* Loop in case we have to retry after enlarging the buffer. */
	do
	{
		va_start(args, fmt);
		done = appendPQExpBufferVA(str, fmt, args);
		va_end(args);
	} while (!done);
}

/*
 * appendPQExpBufferVA
 * Shared guts of printfPQExpBuffer/appendPQExpBuffer.
 * Attempt to format data and append it to str.  Returns true if done
 * (either successful or hard failure), false if need to retry.
 */
bool
appendPQExpBufferVA(PQExpBuffer str, const char *fmt, va_list args)
{
	size_t		avail;
	size_t		needed;
	int			nprinted;

	/*
	 * Try to format the given string into the available space; but if there's
	 * hardly any space, don't bother trying, just enlarge the buffer first.
	 */
	if (str->maxlen > str->len + 16)
	{
		/*
		 * Note: we intentionally leave one byte unused, as a guard against
		 * old broken versions of vsnprintf.
		 */
		avail = str->maxlen - str->len - 1;

		errno = 0;

		nprinted = vsnprintf(str->data + str->len, avail, fmt, args);

		/*
		 * If vsnprintf reports an error other than ENOMEM, fail.
		 */
		if (nprinted < 0 && errno != 0 && errno != ENOMEM)
		{
			markPQExpBufferBroken(str);
			return true;
		}

		/*
		 * Note: some versions of vsnprintf return the number of chars
		 * actually stored, not the total space needed as C99 specifies.  And
		 * at least one returns -1 on failure.  Be conservative about
		 * believing whether the print worked.
		 */
		if (nprinted >= 0 && (size_t) nprinted < avail - 1)
		{
			/* Success.  Note nprinted does not include trailing null. */
			str->len += nprinted;
			return true;
		}

		if (nprinted >= 0 && (size_t) nprinted > avail)
		{
			/*
			 * This appears to be a C99-compliant vsnprintf, so believe its
			 * estimate of the required space. (If it's wrong, the logic will
			 * still work, but we may loop multiple times.)  Note that the
			 * space needed should be only nprinted+1 bytes, but we'd better
			 * allocate one more than that so that the test above will succeed
			 * next time.
			 *
			 * In the corner case where the required space just barely
			 * overflows, fail.
			 */
			if (nprinted > INT_MAX - 2)
			{
				markPQExpBufferBroken(str);
				return true;
			}
			needed = nprinted + 2;
		}
		else
		{
			/*
			 * Buffer overrun, and we don't know how much space is needed.
			 * Estimate twice the previous buffer size, but not more than
			 * INT_MAX.
			 */
			if (avail >= INT_MAX / 2)
				needed = INT_MAX;
			else
				needed = avail * 2;
		}
	}
	else
	{
		/*
		 * We have to guess at how much to enlarge, since we're skipping the
		 * formatting work.
		 */
		needed = 32;
	}

	/* Increase the buffer size and try again. */
	if (!enlargePQExpBuffer(str, needed))
		return true;			/* oops, out of memory */

	return false;
}

/*
 * appendPQExpBufferStr
 * Append the given string to a PQExpBuffer, allocating more space
 * if necessary.
 */
void
appendPQExpBufferStr(PQExpBuffer str, const char *data)
{
	appendBinaryPQExpBuffer(str, data, strlen(data));
}

/*
 * appendPQExpBufferChar
 * Append a single byte to str.
 * Like appendPQExpBuffer(str, "%c", ch) but much faster.
 */
void
appendPQExpBufferChar(PQExpBuffer str, char ch)
{
	/* Make more room if needed */
	if (!enlargePQExpBuffer(str, 1))
		return;

	/* OK, append the character */
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

/*
 * appendBinaryPQExpBuffer
 *
 * Append arbitrary binary data to a PQExpBuffer, allocating more space
 * if necessary.
 */
void
appendBinaryPQExpBuffer(PQExpBuffer str, const char *data, size_t datalen)
{
	/* Make more room if needed */
	if (!enlargePQExpBuffer(str, datalen))
		return;

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data...
	 */
	str->data[str->len] = '\0';
}
