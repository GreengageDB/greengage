/*-------------------------------------------------------------------------
 *
 * copyfromparse.c
 *		Parse CSV/text/binary format for COPY FROM.
 *
 * This file contains routines to parse the text, CSV and binary input
 * formats.  The main entry point is NextCopyFrom(), which parses the
 * next input line and returns it as Datums.
 *
 * In text/CSV mode, the parsing happens in multiple stages:
 *
 * [data source] --> raw_buf --> input_buf --> line_buf --> attribute_buf
 *                1.          2.            3.           4.
 *
 * 1. CopyLoadRawBuf() reads raw data from the input file or client, and
 *    places it into 'raw_buf'.
 *
 * 2. CopyConvertBuf() calls the encoding conversion function to convert
 *    the data in 'raw_buf' from client to server encoding, placing the
 *    converted result in 'input_buf'.
 *
 * 3. CopyReadLine() parses the data in 'input_buf', one line at a time.
 *    It is responsible for finding the next newline marker, taking quote and
 *    escape characters into account according to the COPY options.  The line
 *    is copied into 'line_buf', with quotes and escape characters still
 *    intact.
 *
 * 4. CopyReadAttributesText/CSV() function takes the input line from
 *    'line_buf', and splits it into fields, unescaping the data as required.
 *    The fields are stored in 'attribute_buf', and 'raw_fields' array holds
 *    pointers to each field.
 *
 * If encoding conversion is not required, a shortcut is taken in step 2 to
 * avoid copying the data unnecessarily.  The 'input_buf' pointer is set to
 * point directly to 'raw_buf', so that CopyLoadRawBuf() loads the raw data
 * directly into 'input_buf'.  CopyConvertBuf() then merely validates that
 * the data is valid in the current encoding.
 *
 * In binary mode, the pipeline is much simpler.  Input is loaded into
 * into 'raw_buf', and encoding conversion is done in the datatype-specific
 * receive functions, if required.  'input_buf' and 'line_buf' are not used,
 * but 'attribute_buf' is used as a temporary buffer to hold one attribute's
 * data when it's passed the receive function.
 *
 * 'raw_buf' is always 64 kB in size (RAW_BUF_SIZE).  'input_buf' is also
 * 64 kB (INPUT_BUF_SIZE), if encoding conversion is required.  'line_buf'
 * and 'attribute_buf' are expanded on demand, to hold the longest line
 * encountered so far.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyfromparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')

/*
 * These macros centralize code used to process line_buf and input_buf buffers.
 * They are macros because they often do continue/break control and to avoid
 * function call overhead in tight COPY loops.
 *
 * We must use "if (1)" because the usual "do {...} while(0)" wrapper would
 * prevent the continue/break processing from working.  We end the "if (1)"
 * with "else ((void) 0)" to ensure the "if" does not unintentionally match
 * any "else" in the calling code, and to avoid any compiler warnings about
 * empty statements.  See http://www.cit.gu.edu.au/~anthony/info/C/C.macros.
 */

/*
 * This keeps the character read at the top of the loop in the buffer
 * even if there is more than one read-ahead.
 */
#define IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen) \
if (1) \
{ \
	if (input_buf_ptr + (extralen) >= copy_buf_len && !hit_eof) \
	{ \
		input_buf_ptr = prev_raw_ptr; /* undo fetch */ \
		need_data = true; \
		continue; \
	} \
} else ((void) 0)

/* This consumes the remainder of the buffer and breaks */
#define IF_NEED_REFILL_AND_EOF_BREAK(extralen) \
if (1) \
{ \
	if (input_buf_ptr + (extralen) >= copy_buf_len && hit_eof) \
	{ \
		if (extralen) \
			input_buf_ptr = copy_buf_len; /* consume the partial character */ \
		/* backslash just before EOF, treat as data char */ \
		result = true; \
		break; \
	} \
} else ((void) 0)

/*
 * Transfer any approved data to line_buf; must do this to be sure
 * there is some room in input_buf.
 */
#define REFILL_LINEBUF \
if (1) \
{ \
	if (input_buf_ptr > cstate->input_buf_index) \
	{ \
		appendBinaryStringInfo(&cstate->line_buf, \
							 cstate->input_buf + cstate->input_buf_index, \
							   input_buf_ptr - cstate->input_buf_index); \
		cstate->input_buf_index = input_buf_ptr; \
	} \
} else ((void) 0)

/* Undo any read-ahead and jump out of the block. */
#define NO_END_OF_COPY_GOTO \
if (1) \
{ \
	input_buf_ptr = prev_raw_ptr + 1; \
	goto not_end_of_copy; \
} else ((void) 0)

/* NOTE: there's a copy of this in copyto.c */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";


/* non-export function prototypes */
static bool NextCopyFromRawFieldsX(CopyFromState cstate, char ***fields,
								   int *nfields, int stop_processing_at_field);
static void HandleQDErrorFrame(CopyFromState cstate, char *p, int len);
static bool CopyReadLine(CopyFromState cstate);
static bool CopyReadLineText(CopyFromState cstate);
static int	CopyReadAttributesText(CopyFromState cstate, int stop_processing_at_field);
static int	CopyReadAttributesCSV(CopyFromState cstate, int stop_processing_at_field);
static Datum CopyReadBinaryAttribute(CopyFromState cstate, FmgrInfo *flinfo,
									 Oid typioparam, int32 typmod,
									 bool *isnull);


/* Low-level communications functions */
static inline bool CopyGetInt32(CopyFromState cstate, int32 *val);
static inline bool CopyGetInt16(CopyFromState cstate, int16 *val);
static void CopyLoadRawBuf(CopyFromState cstate);
static void CopyLoadInputBuf(CopyFromState cstate);
static void CopyConsumeBadInputLine(CopyFromState cstate);
static int	CopyReadBinaryData(CopyFromState cstate, char *dest, int nbytes);

void
ReceiveCopyBegin(CopyFromState cstate)
{
	StringInfoData buf;
	int			natts = list_length(cstate->attnumlist);
	int16		format = (cstate->opts.binary ? 1 : 0);
	int			i;

	pq_beginmessage(&buf, 'G');
	pq_sendbyte(&buf, format);	/* overall format */
	pq_sendint16(&buf, natts);
	for (i = 0; i < natts; i++)
		pq_sendint16(&buf, format); /* per-column formats */
	pq_endmessage(&buf);
	cstate->copy_src = COPY_FRONTEND;
	cstate->fe_msgbuf = makeStringInfo();
	/* We *must* flush here to ensure FE knows it can send. */
	pq_flush();
}

void
ReceiveCopyBinaryHeader(CopyFromState cstate)
{
	char		readSig[11];
	int32		tmp;

	/* Signature */
	if (CopyReadBinaryData(cstate, readSig, 11) != 11 ||
		memcmp(readSig, BinarySignature, 11) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("COPY file signature not recognized")));
	/* Flags field */
	if (!CopyGetInt32(cstate, &tmp))
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid COPY file header (missing flags)")));
	if ((tmp & (1 << 16)) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid COPY file header (WITH OIDS)")));
	tmp &= ~(1 << 16);
	if ((tmp >> 16) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unrecognized critical flags in COPY file header")));
	/* Header extension length */
	if (!CopyGetInt32(cstate, &tmp) ||
		tmp < 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid COPY file header (missing length)")));
	/* Skip extension header, if present */
	while (tmp-- > 0)
	{
		if (CopyReadBinaryData(cstate, readSig, 1) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (wrong length)")));
	}
}

/*
 * CopyGetData reads data from the source (file or frontend)
 *
 * We attempt to read at least minread, and at most maxread, bytes from
 * the source.  The actual number of bytes read is returned; if this is
 * less than minread, EOF was detected.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 */
int
CopyGetData(CopyFromState cstate, void *databuf, int datasize)
{
	size_t		bytesread = 0;

	switch (cstate->copy_src)
	{
		case COPY_FILE:
			bytesread = fread(databuf, 1, datasize, cstate->copy_file);

			/*
			 * GGDB: set raw_reached_eof only once a read comes back empty,
			 * exactly like upstream.  Setting it eagerly on feof() -- while
			 * this very read still returned data -- breaks the invariant
			 * CopyReadBinaryData() relies on: after CopyLoadRawBuf() it
			 * treats raw_reached_eof as "the load produced nothing" and
			 * discards a freshly-filled buffer, so any COPY BINARY smaller
			 * than RAW_BUF_SIZE failed with "COPY file signature not
			 * recognized".
			 */
			if (bytesread == 0)
				cstate->raw_reached_eof = true;
			if (ferror(cstate->copy_file))
			{
				if (cstate->is_program)
				{
					int olderrno = errno;

					close_program_pipes(cstate->program_pipes,
										&cstate->copy_file, true);

					/*
					 * If close_program_pipes() didn't throw an error,
					 * the program terminated normally, but closed the
					 * pipe first. Restore errno, and throw an error.
					 */
					errno = olderrno;

					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not read from COPY program: %m")));
				}
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not read from COPY file: %m")));
			}
			break;
		case COPY_FRONTEND:
			while (datasize > 0 && !cstate->raw_reached_eof)
			{
				int			avail;

				while (cstate->fe_msgbuf->cursor >= cstate->fe_msgbuf->len)
				{
					/* Try to receive another message */
					int			mtype;
					int			maxmsglen;

			readmessage:
					HOLD_CANCEL_INTERRUPTS();
					pq_startmsgread();
					mtype = pq_getbyte();
					if (mtype == EOF)
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					/* Validate message type and set packet size limit */
					switch (mtype)
					{
						case 'd':	/* CopyData */
							maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
							break;
						case 'c':	/* CopyDone */
						case 'f':	/* CopyFail */
						case 'H':	/* Flush */
						case 'S':	/* Sync */
							maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("unexpected message type 0x%02X during COPY from stdin",
											mtype)));
							maxmsglen = 0;	/* keep compiler quiet */
							break;
					}
					/* Now collect the message body */
					if (pq_getmessage(cstate->fe_msgbuf, maxmsglen))
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					RESUME_CANCEL_INTERRUPTS();
					/* ... and process it */
					switch (mtype)
					{
						case 'd':	/* CopyData */
							break;
						case 'c':	/* CopyDone */
							/* COPY IN correctly terminated by frontend */
							cstate->raw_reached_eof = true;
							return bytesread;
						case 'f':	/* CopyFail */
							ereport(ERROR,
									(errcode(ERRCODE_QUERY_CANCELED),
									 errmsg("COPY from stdin failed: %s",
											pq_getmsgstring(cstate->fe_msgbuf))));
							break;
						case 'H':	/* Flush */
						case 'S':	/* Sync */

							/*
							 * Ignore Flush/Sync for the convenience of client
							 * libraries (such as libpq) that may send those
							 * without noticing that the command they just
							 * sent was COPY.
							 */
							goto readmessage;
						default:
							Assert(false);	/* NOT REACHED */
					}
				}
				avail = cstate->fe_msgbuf->len - cstate->fe_msgbuf->cursor;
				if (avail > datasize)
					avail = datasize;
				pq_copymsgbytes(cstate->fe_msgbuf, databuf, avail);
				databuf = (void *) ((char *) databuf + avail);
				bytesread += avail;		/* update the count of bytes that were
										 * read so far */
				datasize -= avail;
			}
			break;
		case COPY_CALLBACK:
			bytesread = cstate->data_source_cb(databuf, datasize, datasize,
											   cstate->data_source_cb_extra);
			break;
	}

	return bytesread;
}


/*
 * These functions do apply some data conversion
 */

/*
 * CopyGetInt32 reads an int32 that appears in network byte order
 *
 * Returns true if OK, false if EOF
 */
static inline bool
CopyGetInt32(CopyFromState cstate, int32 *val)
{
	uint32		buf;

	if (CopyReadBinaryData(cstate, (char *) &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int32) pg_ntoh32(buf);
	return true;
}

/*
 * CopyGetInt16 reads an int16 that appears in network byte order
 */
static inline bool
CopyGetInt16(CopyFromState cstate, int16 *val)
{
	uint16		buf;

	if (CopyReadBinaryData(cstate, (char *) &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int16) pg_ntoh16(buf);
	return true;
}


/*
 * Perform encoding conversion on data in 'raw_buf', writing the converted
 * data into 'input_buf'.
 *
 * On entry, there must be some data to convert in 'raw_buf'.
 */
static void
CopyConvertBuf(CopyFromState cstate)
{
	/*
	 * If the file and server encoding are the same, no encoding conversion is
	 * required.  However, we still need to verify that the input is valid for
	 * the encoding.
	 */
	if (!cstate->need_transcoding)
	{
		/*
		 * When conversion is not required, input_buf and raw_buf are the
		 * same.  raw_buf_len is the total number of bytes in the buffer, and
		 * input_buf_len tracks how many of those bytes have already been
		 * verified.
		 */
		int			preverifiedlen = cstate->input_buf_len;
		int			unverifiedlen = cstate->raw_buf_len - cstate->input_buf_len;
		int			nverified;

		if (unverifiedlen == 0)
		{
			/*
			 * If no more raw data is coming, report the EOF to the caller.
			 */
			if (cstate->raw_reached_eof)
				cstate->input_reached_eof = true;
			return;
		}

		/*
		 * Verify the new data, including any residual unverified bytes from
		 * previous round.
		 */
		nverified = pg_encoding_verifymbstr(cstate->file_encoding,
											cstate->raw_buf + preverifiedlen,
											unverifiedlen);
		if (nverified == 0)
		{
			/*
			 * Could not verify anything.
			 *
			 * If there is no more raw input data coming, it means that there
			 * was an incomplete multi-byte sequence at the end.  Also, if
			 * there's "enough" input left, we should be able to verify at
			 * least one character, and a failure to do so means that we've
			 * hit an invalid byte sequence.
			 */
			if (cstate->raw_reached_eof || unverifiedlen >= pg_database_encoding_max_length())
				cstate->input_reached_error = true;
			return;
		}
		cstate->input_buf_len += nverified;
	}
	else
	{
		/*
		 * Encoding conversion is needed.
		 */
		int			nbytes;
		unsigned char *src;
		int			srclen;
		unsigned char *dst;
		int			dstlen;
		int			convertedlen;

		if (RAW_BUF_BYTES(cstate) == 0)
		{
			/*
			 * If no more raw data is coming, report the EOF to the caller.
			 */
			if (cstate->raw_reached_eof)
				cstate->input_reached_eof = true;
			return;
		}

		/*
		 * First, copy down any unprocessed data.
		 */
		nbytes = INPUT_BUF_BYTES(cstate);
		if (nbytes > 0 && cstate->input_buf_index > 0)
			memmove(cstate->input_buf, cstate->input_buf + cstate->input_buf_index,
					nbytes);
		cstate->input_buf_index = 0;
		cstate->input_buf_len = nbytes;
		cstate->input_buf[nbytes] = '\0';

		src = (unsigned char *) cstate->raw_buf + cstate->raw_buf_index;
		srclen = cstate->raw_buf_len - cstate->raw_buf_index;
		dst = (unsigned char *) cstate->input_buf + cstate->input_buf_len;
		dstlen = INPUT_BUF_SIZE - cstate->input_buf_len + 1;

		/*
		 * Do the conversion.  This might stop short, if there is an invalid
		 * byte sequence in the input.  We'll convert as much as we can in
		 * that case.
		 *
		 * Note: Even if we hit an invalid byte sequence, we don't report the
		 * error until all the valid bytes have been consumed.  The input
		 * might contain an end-of-input marker (\.), and we don't want to
		 * report an error if the invalid byte sequence is after the
		 * end-of-input marker.  We might unnecessarily convert some data
		 * after the end-of-input marker as long as it's valid for the
		 * encoding, but that's harmless.
		 */
		convertedlen = pg_do_encoding_conversion_buf(cstate->conversion_proc,
													 cstate->file_encoding,
													 GetDatabaseEncoding(),
													 src, srclen,
													 dst, dstlen,
													 true);
		if (convertedlen == 0)
		{
			/*
			 * Could not convert anything.  If there is no more raw input data
			 * coming, it means that there was an incomplete multi-byte
			 * sequence at the end.  Also, if there is plenty of input left,
			 * we should be able to convert at least one character, so a
			 * failure to do so must mean that we've hit a byte sequence
			 * that's invalid.
			 */
			if (cstate->raw_reached_eof || srclen >= MAX_CONVERSION_INPUT_LENGTH)
				cstate->input_reached_error = true;
			return;
		}
		cstate->raw_buf_index += convertedlen;
		cstate->input_buf_len += strlen((char *) dst);
	}
}

/*
 * GPDB: one step of the CSV quote/escape state machine, shared by
 * CopyReadLineText() (its main byte scan) and CopyConsumeBadInputLine()
 * (which replays the quoting state over a rejected line's already-consumed
 * prefix before scanning its raw tail).
 */
static inline void
CopyCSVQuoteStep(char c, char quotec, char escapec,
				 bool *in_quote, bool *last_was_esc)
{
	if (*in_quote && c == escapec)
		*last_was_esc = !*last_was_esc;
	if (c == quotec && !*last_was_esc)
		*in_quote = !*in_quote;
	if (c != escapec)
		*last_was_esc = false;
}

/*
 * GPDB: helper for CopyConsumeBadInputLine().  Flush the scanned raw
 * segment [*segstart, *pos) to line_buf, consume it from the pipeline, and
 * load more raw data.  On return, *pos and *segstart point at the first
 * not-yet-scanned byte (the buffer contents may have moved).
 */
static void
CopyConsumeBadRefill(CopyFromState cstate, int *pos, int *segstart)
{
	if (*pos > *segstart)
		appendBinaryStringInfo(&cstate->line_buf,
							   cstate->raw_buf + *segstart,
							   *pos - *segstart);

	/* consume the flushed bytes */
	cstate->raw_buf_index = *pos;
	if (cstate->raw_buf == cstate->input_buf)
	{
		/*
		 * input_buf aliases raw_buf: mark the consumed bytes as verified,
		 * to keep the aliased state coherent (see CopyLoadRawBuf's
		 * assertions).
		 */
		cstate->input_buf_index = *pos;
		cstate->input_buf_len = *pos;
	}

	CopyLoadRawBuf(cstate);
	*pos = *segstart = cstate->raw_buf_index;
}

/*
 * GPDB: single-row error handling (SREH) support for encoding
 * verification/conversion errors.
 *
 * The upstream pipeline verifies/converts the input in chunks
 * (CopyConvertBuf), before line splitting.  When it hits an invalid byte
 * sequence it reports an error without consuming the offending bytes.
 * That is fine for the all-or-nothing upstream behavior, but under SREH
 * the error is caught (HandleCopyError) and NextCopyFrom() retries: the
 * retry would hit the very same bytes again and make no forward progress,
 * rejecting a phantom (empty) row with an ever-increasing line number on
 * each retry, until the reject limit kills the COPY.
 *
 * To restore the pre-PG14, per-line conversion semantics, this function
 * attributes the error to the physical input line that contains the
 * offending byte sequence:
 *
 * 1. It appends to line_buf the not-yet-consumed bytes of the current
 *    line: first the already-verified bytes still in input_buf, then the
 *    raw (unverified/unconverted) bytes up to, but excluding, the line
 *    terminator (or EOF).  HandleCopyError then logs line_buf as the
 *    rejected row, under the current line number.
 *
 * 2. It consumes that line, terminator included, from the pipeline
 *    (raw_buf/input_buf), and clears input_reached_error, so that
 *    verification resumes at the start of the next line and the retry
 *    makes forward progress.
 *
 * The terminator search mirrors CopyReadLineText's GPDB semantics: the
 * line ends only at the configured/detected EOL (bare \r or \n are data
 * in EOL_CRNL mode, etc.), the EOL style is locked in from this line when
 * it was not detected yet, quoted CSV fields may contain newlines (the
 * quoting state of the line's already-consumed prefix is replayed), and
 * in text mode a backslash escapes the following byte.  The end-of-copy
 * marker (\.) is deliberately not recognized: this path only runs on an
 * encoding error and \. is always valid ASCII, so the offending line
 * always ends at its own newline and a following "\.\n" is handled
 * normally by the next NextCopyFrom().
 *
 * Note: in the transcoding case, the already-converted prefix of the line
 * (in line_buf/input_buf) is in the server encoding; its raw bytes have
 * already been consumed from raw_buf and are no longer available.  The
 * logged row is therefore the converted prefix plus the raw tail.  The
 * row is logged with is_server_enc = false (raw bytes), which is accurate
 * for the offending bytes themselves.
 */
static void
CopyConsumeBadInputLine(CopyFromState cstate)
{
	bool		in_quote = false;
	bool		last_was_esc = false;
	char		quotec = '\0';
	char		escapec = '\0';
	int			pos;
	int			segstart;
	int			eol_len = 0;

	if (cstate->opts.csv_mode)
	{
		int			i;

		quotec = cstate->opts.quote[0];
		escapec = cstate->opts.escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';

		/*
		 * Replay the CSV quoting state over the part of the line that has
		 * been read so far, to know whether the raw tail starts inside a
		 * quoted field.  (The quote/escape characters are ASCII, and both
		 * the server encoding and the aliased no-transcoding raw data
		 * cannot embed ASCII-equal bytes in multi-byte characters, so a
		 * byte-wise replay matches what CopyReadLineText saw.)
		 */
		for (i = 0; i < cstate->line_buf.len; i++)
			CopyCSVQuoteStep(cstate->line_buf.data[i], quotec, escapec,
							 &in_quote, &last_was_esc);
		for (i = cstate->input_buf_index; i < cstate->input_buf_len; i++)
		{
			char		c = cstate->input_buf[i];

			CopyCSVQuoteStep(c, quotec, escapec, &in_quote, &last_was_esc);

			/*
			 * CopyReadLineText() never scanned this verified-but-unconsumed
			 * tail of input_buf, so its embedded-newline line count did not
			 * run over it; do it here, like the main scan loop below.  (In
			 * the paths that reach here INPUT_BUF_BYTES is 0, so this loop
			 * is normally empty -- this is for symmetry, not a live case.)
			 */
			if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}
	}

	/*
	 * Transfer the verified-but-unconsumed bytes of the current line to
	 * line_buf, and consume them.
	 */
	if (INPUT_BUF_BYTES(cstate) > 0)
	{
		appendBinaryStringInfo(&cstate->line_buf,
							   cstate->input_buf + cstate->input_buf_index,
							   INPUT_BUF_BYTES(cstate));
		cstate->input_buf_index = cstate->input_buf_len;
	}

	/*
	 * Find where the offending raw data begins.
	 */
	if (!cstate->need_transcoding)
	{
		/*
		 * input_buf aliases raw_buf: everything up to input_buf_len has
		 * been verified (and, by now, consumed); the invalid byte is at
		 * offset input_buf_len.  Keep raw_buf_index in sync with
		 * input_buf_index, like CopyLoadInputBuf does.
		 */
		Assert(cstate->raw_buf == cstate->input_buf);
		cstate->raw_buf_index = cstate->input_buf_index;
		pos = cstate->raw_buf_index;
	}
	else
	{
		/* raw_buf_index points at the unconvertible byte sequence. */
		pos = cstate->raw_buf_index;
	}
	segstart = pos;

	/*
	 * Scan the raw data for the line terminator, loading more data as
	 * needed.
	 */
	for (;;)
	{
		char		c;

		if (pos >= cstate->raw_buf_len)
		{
			if (cstate->raw_reached_eof)
				break;			/* the bad line is terminated by EOF */
			CopyConsumeBadRefill(cstate, &pos, &segstart);
			continue;
		}

		c = cstate->raw_buf[pos];

		/*
		 * '\r' may need a one-byte lookahead for a following '\n', and in
		 * text mode a backslash escapes the next byte: make sure the next
		 * byte is available before processing 'c'.  (At EOF, the missing
		 * lookahead byte reads as the '\0' pad, like in CopyReadLineText.)
		 */
		if (((c == '\r' && (cstate->eol_type == EOL_CRNL ||
							cstate->eol_type == EOL_UNKNOWN)) ||
			 (c == '\\' && !cstate->opts.csv_mode)) &&
			pos + 1 >= cstate->raw_buf_len && !cstate->raw_reached_eof)
		{
			CopyConsumeBadRefill(cstate, &pos, &segstart);
			continue;
		}

		if (cstate->opts.csv_mode)
		{
			CopyCSVQuoteStep(c, quotec, escapec, &in_quote, &last_was_esc);

			/*
			 * Keep the line count in sync for newlines embedded in quoted
			 * fields, like CopyReadLineText does.
			 */
			if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}

		if (!cstate->opts.csv_mode || !in_quote)
		{
			/*
			 * Check for the line terminator.  When the EOL style has not
			 * been detected yet (a bad first line), lock it in exactly like
			 * CopyReadLineText() does, so the rest of the file is split the
			 * same way whether or not the first line was rejected.
			 */
			if (c == '\n' &&
				(cstate->eol_type == EOL_NL ||
				 cstate->eol_type == EOL_UNKNOWN))
			{
				if (cstate->eol_type == EOL_UNKNOWN)
					cstate->eol_type = EOL_NL;
				eol_len = 1;
				break;
			}
			if (c == '\r')
			{
				if (cstate->eol_type == EOL_CR)
				{
					eol_len = 1;
					break;
				}
				if (cstate->eol_type == EOL_CRNL ||
					cstate->eol_type == EOL_UNKNOWN)
				{
					if (pos + 1 < cstate->raw_buf_len &&
						cstate->raw_buf[pos + 1] == '\n')
					{
						cstate->eol_type = EOL_CRNL;
						eol_len = 2;
						break;
					}
					if (cstate->eol_type == EOL_UNKNOWN)
					{
						/* bare CR ends the line, like CopyReadLineText */
						cstate->eol_type = EOL_CR;
						eol_len = 1;
						break;
					}
					/* EOL_CRNL: a bare \r is data */
				}
				/* EOL_NL: a bare \r is data */
			}

			/*
			 * In text mode, a backslash escapes the next byte: skip it, so
			 * that an escaped CR/LF is not taken for the terminator.  A
			 * backslash directly followed by the line terminator ends the
			 * line, with the backslash as data (like CopyReadLineText, see
			 * github issue 12454): the terminator branch above handles that
			 * on the next iteration.
			 */
			if (c == '\\' && !cstate->opts.csv_mode &&
				pos + 1 < cstate->raw_buf_len)
			{
				char		c2 = cstate->raw_buf[pos + 1];

				if (c2 != '\n' && c2 != '\r')
					pos++;		/* skip the escaped byte too */
			}
		}

		pos++;
	}

	/*
	 * Transfer the rest of the line to line_buf, excluding the terminator,
	 * and consume the line, terminator included.
	 */
	if (pos > segstart)
		appendBinaryStringInfo(&cstate->line_buf,
							   cstate->raw_buf + segstart,
							   pos - segstart);

	pos += eol_len;
	Assert(pos <= cstate->raw_buf_len);
	cstate->raw_buf_index = pos;
	if (cstate->raw_buf == cstate->input_buf)
	{
		/*
		 * Mark the consumed bytes as verified, so that verification
		 * resumes at the byte after the terminator.
		 */
		cstate->input_buf_index = pos;
		cstate->input_buf_len = pos;
	}

	/* We have consumed past the error; let conversion resume. */
	cstate->input_reached_error = false;

	/*
	 * The rejected row contains raw, unconverted bytes: log it as raw
	 * bytes (is_server_enc = false), like the pre-PG14 per-line conversion
	 * model did.
	 */
	cstate->line_buf_converted = false;
	cstate->line_buf_valid = true;
}

/*
 * Report an encoding or conversion error.
 */
static void
CopyConversionError(CopyFromState cstate)
{
	Assert(cstate->raw_buf_len > 0);
	Assert(cstate->input_reached_error);

	/*
	 * GPDB: under single-row error handling, attribute the error to the
	 * current physical input line: place the line's raw bytes in line_buf
	 * for the error handler (HandleCopyError) to log, and consume the line
	 * so that the next NextCopyFrom() attempt makes forward progress.  See
	 * CopyConsumeBadInputLine.
	 *
	 * The offending bytes must be saved before consuming the line, because
	 * consuming may shift or overwrite the buffer contents that the error
	 * report needs.
	 */
	if (cstate->cdbsreh)
	{
		char		errbytes[64];
		int			errbyteslen;
		const char *errsrc;

		if (!cstate->need_transcoding)
			errsrc = cstate->raw_buf + cstate->input_buf_len;
		else
			errsrc = cstate->raw_buf + cstate->raw_buf_index;
		errbyteslen = Min(cstate->raw_buf_len - (int) (errsrc - cstate->raw_buf),
						  (int) sizeof(errbytes));
		Assert(errbyteslen > 0);
		memcpy(errbytes, errsrc, errbyteslen);

		CopyConsumeBadInputLine(cstate);

		if (!cstate->need_transcoding)
			report_invalid_encoding(cstate->file_encoding,
									errbytes, errbyteslen);
		else
		{
			char		dstbuf[sizeof(errbytes) * MAX_CONVERSION_GROWTH + 1];

			/*
			 * Have the conversion routine throw the error, for a more
			 * specific error message.  The saved bytes begin at the
			 * unconvertible byte sequence, so it reports right away.
			 */
			(void) pg_do_encoding_conversion_buf(cstate->conversion_proc,
												 cstate->file_encoding,
												 GetDatabaseEncoding(),
												 (unsigned char *) errbytes,
												 errbyteslen,
												 (unsigned char *) dstbuf,
												 sizeof(dstbuf),
												 false);

			/*
			 * The conversion routine should have reported an error, so
			 * this should not be reached.
			 */
			elog(ERROR, "encoding conversion failed without error");
		}
	}

	if (!cstate->need_transcoding)
	{
		/*
		 * Everything up to input_buf_len was successfully verified, and
		 * input_buf_len points to the invalid or incomplete character.
		 */
		report_invalid_encoding(cstate->file_encoding,
								cstate->raw_buf + cstate->input_buf_len,
								cstate->raw_buf_len - cstate->input_buf_len);
	}
	else
	{
		/*
		 * raw_buf_index points to the invalid or untranslatable character. We
		 * let the conversion routine report the error, because it can provide
		 * a more specific error message than we could here.  An earlier call
		 * to the conversion routine in CopyConvertBuf() detected that there
		 * is an error, now we call the conversion routine again with
		 * noError=false, to have it throw the error.
		 */
		unsigned char *src;
		int			srclen;
		unsigned char *dst;
		int			dstlen;

		src = (unsigned char *) cstate->raw_buf + cstate->raw_buf_index;
		srclen = cstate->raw_buf_len - cstate->raw_buf_index;
		dst = (unsigned char *) cstate->input_buf + cstate->input_buf_len;
		dstlen = INPUT_BUF_SIZE - cstate->input_buf_len + 1;

		(void) pg_do_encoding_conversion_buf(cstate->conversion_proc,
											 cstate->file_encoding,
											 GetDatabaseEncoding(),
											 src, srclen,
											 dst, dstlen,
											 false);

		/*
		 * The conversion routine should have reported an error, so this
		 * should not be reached.
		 */
		elog(ERROR, "encoding conversion failed without error");
	}
}

/*
 * Load more data from data source to raw_buf.
 *
 * If RAW_BUF_BYTES(cstate) > 0, the unprocessed bytes are moved to the
 * beginning of the buffer, and we load new data after that.
 */
static void
CopyLoadRawBuf(CopyFromState cstate)
{
	int			nbytes;
	int			inbytes;

	/*
	 * In text mode, if encoding conversion is not required, raw_buf and
	 * input_buf point to the same buffer.  Their len/index better agree, too.
	 */
	if (cstate->raw_buf == cstate->input_buf)
	{
		Assert(!cstate->need_transcoding);
		Assert(cstate->raw_buf_index == cstate->input_buf_index);
		Assert(cstate->input_buf_len <= cstate->raw_buf_len);
	}

	/*
	 * Copy down the unprocessed data if any.
	 */
	nbytes = RAW_BUF_BYTES(cstate);
	if (nbytes > 0 && cstate->raw_buf_index > 0)
		memmove(cstate->raw_buf, cstate->raw_buf + cstate->raw_buf_index,
				nbytes);
	cstate->raw_buf_len -= cstate->raw_buf_index;
	cstate->raw_buf_index = 0;

	/*
	 * If raw_buf and input_buf are in fact the same buffer, adjust the
	 * input_buf variables, too.
	 */
	if (cstate->raw_buf == cstate->input_buf)
	{
		cstate->input_buf_len -= cstate->input_buf_index;
		cstate->input_buf_index = 0;
	}

	/* Load more data */
	inbytes = CopyGetData(cstate, cstate->raw_buf + cstate->raw_buf_len,
						  RAW_BUF_SIZE - cstate->raw_buf_len);
	nbytes += inbytes;
	cstate->raw_buf[nbytes] = '\0';
	cstate->raw_buf_len = nbytes;

	cstate->bytes_processed += inbytes;
	pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate->bytes_processed);

	if (inbytes == 0)
		cstate->raw_reached_eof = true;
}

/*
 * CopyLoadInputBuf loads some more data into input_buf
 *
 * On return, at least one more input character is loaded into
 * input_buf, or input_reached_eof is set.
 *
 * If INPUT_BUF_BYTES(cstate) > 0, the unprocessed bytes are moved to the start
 * of the buffer and then we load more data after that.
 */
static void
CopyLoadInputBuf(CopyFromState cstate)
{
	int			nbytes = INPUT_BUF_BYTES(cstate);

	/*
	 * The caller has updated input_buf_index to indicate how much of the
	 * input has been consumed and isn't needed anymore.  If input_buf is the
	 * same physical area as raw_buf, update raw_buf_index accordingly.
	 */
	if (cstate->raw_buf == cstate->input_buf)
	{
		Assert(!cstate->need_transcoding);
		Assert(cstate->input_buf_index >= cstate->raw_buf_index);
		cstate->raw_buf_index = cstate->input_buf_index;
	}

	for (;;)
	{
		/* If we now have some unconverted data, try to convert it */
		CopyConvertBuf(cstate);

		/* If we now have some more input bytes ready, return them */
		if (INPUT_BUF_BYTES(cstate) > nbytes)
			return;

		/*
		 * If we reached an invalid byte sequence, or we're at an incomplete
		 * multi-byte character but there is no more raw input data, report
		 * conversion error.
		 */
		if (cstate->input_reached_error)
			CopyConversionError(cstate);

		/* no more input, and everything has been converted */
		if (cstate->input_reached_eof)
			break;

		/* Try to load more raw data */
		Assert(!cstate->raw_reached_eof);
		CopyLoadRawBuf(cstate);
	}
}

/*
 * CopyReadBinaryData
 *
 * Reads up to 'nbytes' bytes from cstate->copy_file via cstate->raw_buf
 * and writes them to 'dest'.  Returns the number of bytes read (which
 * would be less than 'nbytes' only if we reach EOF).
 */
static int
CopyReadBinaryData(CopyFromState cstate, char *dest, int nbytes)
{
	int			copied_bytes = 0;

	if (RAW_BUF_BYTES(cstate) >= nbytes)
	{
		/* Enough bytes are present in the buffer. */
		memcpy(dest, cstate->raw_buf + cstate->raw_buf_index, nbytes);
		cstate->raw_buf_index += nbytes;
		copied_bytes = nbytes;
	}
	else
	{
		/*
		 * Not enough bytes in the buffer, so must read from the file.  Need
		 * to loop since 'nbytes' could be larger than the buffer size.
		 */
		do
		{
			int			copy_bytes;

			/* Load more data if buffer is empty. */
			if (RAW_BUF_BYTES(cstate) == 0)
			{
				CopyLoadRawBuf(cstate);
				if (cstate->raw_reached_eof)
					break;		/* EOF */
			}

			/* Transfer some bytes. */
			copy_bytes = Min(nbytes - copied_bytes, RAW_BUF_BYTES(cstate));
			memcpy(dest, cstate->raw_buf + cstate->raw_buf_index, copy_bytes);
			cstate->raw_buf_index += copy_bytes;
			dest += copy_bytes;
			copied_bytes += copy_bytes;
		} while (copied_bytes < nbytes);
	}

	return copied_bytes;
}

/*
 * Read raw fields in the next line for COPY FROM in text or csv mode.
 * Return false if no more lines.
 *
 * An internal temporary buffer is returned via 'fields'. It is valid until
 * the next call of the function. Since the function returns all raw fields
 * in the input file, 'nfields' could be different from the number of columns
 * in the relation.
 *
 * NOTE: force_not_null option are not applied to the returned fields.
 */
bool
NextCopyFromRawFields(CopyFromState cstate, char ***fields, int *nfields)
{
	return NextCopyFromRawFieldsX(cstate, fields, nfields, -1);
}

static bool
NextCopyFromRawFieldsX(CopyFromState cstate, char ***fields, int *nfields,
					   int stop_processing_at_field)
{
	int			fldct;
	bool		done;

	/* only available for text or csv input */
	Assert(!cstate->opts.binary);

	/* on input just throw the header line away */
	if (cstate->cur_lineno == 0 && cstate->opts.header_line)
	{
		cstate->cur_lineno++;
		if (CopyReadLine(cstate))
			return false;		/* done */
	}

	cstate->cur_lineno++;

	/* Actually read the line into memory here */
	done = CopyReadLine(cstate);

	/*
	 * EOF at start of line means we're done.  If we see EOF after some
	 * characters, we act as though it was newline followed by EOF, ie,
	 * process the line and then exit loop on next iteration.
	 */
	if (done && cstate->line_buf.len == 0)
		return false;

	/* Parse the line into de-escaped field values */
	if (cstate->opts.csv_mode)
		fldct = CopyReadAttributesCSV(cstate, stop_processing_at_field);
	else
		fldct = CopyReadAttributesText(cstate, stop_processing_at_field);

	*fields = cstate->raw_fields;
	*nfields = fldct;
	return true;
}

bool
NextCopyFrom(CopyFromState cstate, ExprContext *econtext,
			   Datum *values, bool *nulls)
{
	if (!cstate->cdbsreh)
		return NextCopyFromX(cstate, econtext, values, nulls);
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;

		for (;;)
		{
			bool		got_error = false;
			bool		result = false;

			PG_TRY();
			{
				result = NextCopyFromX(cstate, econtext, values, nulls);
			}
			PG_CATCH();
			{
				HandleCopyError(cstate); /* cdbsreh->processed is updated inside here */
				got_error = true;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_END_TRY();

			if (result)
				cstate->cdbsreh->processed++;

			if (!got_error)
				return result;
		}
	}
}

/*
 * A data error happened. This code block will always be inside a PG_CATCH()
 * block right when a higher stack level produced an error. We handle the error
 * by checking which error mode is set (SREH or all-or-nothing) and do the right
 * thing accordingly. Note that we MUST have this code in a macro (as opposed
 * to a function) as elog_dismiss() has to be inlined with PG_CATCH in order to
 * access local error state variables.
 *
 * changing me? take a look at FILEAM_HANDLE_ERROR in fileam.c as well.
 */
void
HandleCopyError(CopyFromState cstate)
{
	if (cstate->errMode == ALL_OR_NOTHING)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	/* SREH must only handle data errors. all other errors must not be caught */
	if (ERRCODE_TO_CATEGORY(elog_geterrcode()) != ERRCODE_DATA_EXCEPTION)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	else
	{
		/* SREH - release error state and handle error */
		MemoryContext oldcontext;
		ErrorData	*edata;
		char	   *errormsg;
		CdbSreh	   *cdbsreh = cstate->cdbsreh;

		cdbsreh->processed++;

		oldcontext = MemoryContextSwitchTo(cstate->cdbsreh->badrowcontext);

		/* save a copy of the error info */
		edata = CopyErrorData();

		FlushErrorState();

		/*
		 * set the error message. Use original msg and add column name if available.
		 * We do this even if we're not logging the errors, because
		 * ErrorIfRejectLimit() below will use this information in the error message,
		 * if the error count is reached.
		 */
		cdbsreh->rawdata->cursor = 0;
		cdbsreh->rawdata->data = cstate->line_buf.data;
		cdbsreh->rawdata->len = cstate->line_buf.len;

		cdbsreh->is_server_enc = cstate->line_buf_converted;
		cdbsreh->linenumber = cstate->cur_lineno;
		if (cstate->cur_attname)
		{
			errormsg =  psprintf("%s, column %s",
								 edata->message, cstate->cur_attname);
		}
		else
		{
			errormsg = edata->message;
		}
		cstate->cdbsreh->errmsg = errormsg;

		if (IS_LOG_TO_FILE(cstate->cdbsreh->logerrors))
		{
			if (Gp_role == GP_ROLE_DISPATCH && !cstate->opts.on_segment)
			{
				cstate->cdbsreh->rejectcount++;

				SendCopyFromForwardedError(cstate, cstate->cdbCopy, errormsg);
			}
			else
			{
				/* after all the prep work let cdbsreh do the real work */
				if (Gp_role == GP_ROLE_DISPATCH)
				{
					cstate->cdbsreh->rejectcount++;
				}
				else
				{
					HandleSingleRowError(cstate->cdbsreh);
				}
			}
		}
		else
			cstate->cdbsreh->rejectcount++;

		ErrorIfRejectLimitReached(cstate->cdbsreh);

		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(cstate->cdbsreh->badrowcontext);
	}
}

/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 */
bool
NextCopyFromX(CopyFromState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;
	List	   *attnumlist;
	int			stop_processing_at_field;

	/*
	 * Figure out what fields we're going to process in this process.
	 *
	 * In the QD, set 'stop_processing_at_field' so that we only those
	 * fields that are needed in the QD.
	 */
	switch (cstate->dispatch_mode)
	{
		case COPY_DIRECT:
			stop_processing_at_field = -1;
			attnumlist = cstate->attnumlist;
			break;

		case COPY_DISPATCH:
			stop_processing_at_field = cstate->first_qe_processed_field;
			attnumlist = cstate->qd_attnumlist;
			break;

		case COPY_EXECUTOR:
			stop_processing_at_field = -1;
			attnumlist = cstate->qe_attnumlist;
			break;

		default:
			elog(ERROR, "unexpected COPY dispatch mode %d", cstate->dispatch_mode);
	}

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(attnumlist);

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	if (!cstate->opts.binary)
	{
		char	  **field_strings;
		ListCell   *cur;
		int			fldct;
		int			fieldno;
		char	   *string;

		/* read raw fields in the next line */
		if (cstate->dispatch_mode != COPY_EXECUTOR)
		{
			if (!NextCopyFromRawFieldsX(cstate, &field_strings, &fldct,
										stop_processing_at_field))
				return false;
		}
		else
		{
			/*
			 * We have received the raw line from the QD, and we just
			 * need to split it into raw fields.
			 */
			if (cstate->stopped_processing_at_delim &&
				cstate->line_buf.cursor <= cstate->line_buf.len)
			{
				if (cstate->opts.csv_mode)
					fldct = CopyReadAttributesCSV(cstate, -1);
				else
					fldct = CopyReadAttributesText(cstate, -1);
			}
			else
				fldct = 0;
			field_strings = cstate->raw_fields;
		}

		/* check for overflowing fields */
		if (attr_count > 0 && fldct > attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));

		/*
		 * A completely empty line is not allowed with FILL MISSING FIELDS. Without
		 * FILL MISSING FIELDS, it's almost surely an error, but not always:
		 * a table with a single text column, for example, needs to accept empty
		 * lines.
		 */
		if (cstate->line_buf.len == 0 &&
			cstate->opts.fill_missing &&
			list_length(cstate->attnumlist) > 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("missing data for column \"%s\", found empty data line",
							NameStr(TupleDescAttr(tupDesc, 1)->attname))));
		}

		fieldno = 0;

		/* Loop to read the user attributes on the line. */
		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			if (fieldno >= fldct)
			{
				/*
				 * Some attributes are missing. In FILL MISSING FIELDS mode,
				 * treat them as NULLs.
				 */
				if (!cstate->opts.fill_missing)
					ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for column \"%s\"",
								NameStr(att->attname))));
				fieldno++;
				string = NULL;
			}
			else
				string = field_strings[fieldno++];

			if (cstate->convert_select_flags &&
				!cstate->convert_select_flags[m])
			{
				/* ignore input field, leaving column as NULL */
				continue;
			}

			if (cstate->opts.csv_mode)
			{
				if (string == NULL &&
					cstate->opts.force_notnull_flags[m])
				{
					/*
					 * FORCE_NOT_NULL option is set and column is NULL -
					 * convert it to the NULL string.
					 */
					string = cstate->opts.null_print;
				}
				else if (string != NULL && cstate->opts.force_null_flags[m]
						 && strcmp(string, cstate->opts.null_print) == 0)
				{
					/*
					 * FORCE_NULL option is set and column matches the NULL
					 * string. It must have been quoted, or otherwise the
					 * string would already have been set to NULL. Convert it
					 * to NULL as specified.
					 */
					string = NULL;
				}
			}

			cstate->cur_attname = NameStr(att->attname);
			cstate->cur_attval = string;
			values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  att->atttypmod);
			if (string != NULL)
				nulls[m] = false;
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}

		Assert(fieldno == attr_count);
	}
	else if (attr_count)
	{
		/* binary */
		int16		fld_count;
		ListCell   *cur;

		cstate->cur_lineno++;

		if (!CopyGetInt16(cstate, &fld_count))
		{
			/* EOF detected (end of file, or protocol-level EOF) */
			return false;
		}

		if (fld_count == -1)
		{
			/*
			 * Received EOF marker.  Wait for the protocol-level EOF, and
			 * complain if it doesn't come immediately.  In COPY FROM STDIN,
			 * this ensures that we correctly handle CopyFail, if client
			 * chooses to send that now.  When copying from file, we could
			 * ignore the rest of the file like in text mode, but we choose to
			 * be consistent with the COPY FROM STDIN case.
			 */
			char		dummy;

			if (CopyReadBinaryData(cstate, &dummy, 1) > 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("received copy data after EOF marker")));
			return false;
		}

		if (fld_count != attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("row field count is %d, expected %d",
							(int) fld_count, attr_count)));

		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			cstate->cur_attname = NameStr(att->attname);
			values[m] = CopyReadBinaryAttribute(cstate,
												&in_functions[m],
												typioparams[m],
												att->atttypmod,
												&nulls[m]);
			cstate->cur_attname = NULL;
		}
	}

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.  Anything not processed here or above will
	 * remain NULL.
	 *
	 * GPDB: The defaults are always computed in the QD, and are included
	 * in the QD->QE stream as pre-computed Datums. Funny indentation, to
	 * keep the indentation of the code inside the same as in upstream.
	 * (We could improve this, and compute immutable defaults that don't
	 * affect which segment the row belongs to, in the QE.)
	 */
  if (cstate->dispatch_mode != COPY_EXECUTOR)
  {
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]]);
	}
  }

	return true;
}

/*
 * Like NextCopyFrom(), but used in the QD, when we want to parse the
 * input line only partially. We only want to parse enough fields needed
 * to determine which target segment to forward the row to.
 *
 * (The logic is actually within NextCopyFrom(). This is a separate
 * function just for symmetry with NextCopyFromExecute()).
 */
bool
NextCopyFromDispatch(CopyFromState cstate, ExprContext *econtext,
					 Datum *values, bool *nulls)
{
	return NextCopyFrom(cstate, econtext, values, nulls);
}

/*
 * Like NextCopyFrom(), but used in the QE, when we're reading pre-processed
 * rows from the QD.
 */
bool
NextCopyFromExecute(CopyFromState cstate, ExprContext *econtext, Datum *values, bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count;
	FormData_pg_attribute *attr;
	int			i;
	copy_from_dispatch_row frame;
	int			r;
	bool		got_error;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);

	/*
	 * The code below reads the 'copy_from_dispatch_row' struct, and only
	 * then checks if it was actually a 'copy_from_dispatch_error' struct.
	 * That only works when 'copy_from_dispatch_error' is at least as large as
	 * 'copy_from_dispatch_row'.
	 */
	StaticAssertStmt(SizeOfCopyFromDispatchError >= SizeOfCopyFromDispatchRow,
					 "copy_from_dispatch_error must not be smaller than copy_from_dispatch_row");

	/*
	 * If we encounter an error while parsing the row (or we receive a row from
	 * the QD that was already marked as an erroneous row), we loop back here
	 * until we get a good row.
	 */
retry:
	got_error = false;

	r = CopyGetData(cstate, (char *) &frame, SizeOfCopyFromDispatchRow);
	if (r == 0)
		return false;
	if (r != SizeOfCopyFromDispatchRow)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	if (frame.lineno == -1)
	{
		HandleQDErrorFrame(cstate, (char *) &frame, SizeOfCopyFromDispatchRow);
		goto retry;
	}

	/* Prepare for parsing the input line */
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	/* check for overflowing fields */
	if (frame.fld_count < 0 || frame.fld_count > num_phys_attrs)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("extra data after last expected column")));

	/*
	 * Read the input line into 'line_buf'.
	 */
	resetStringInfo(&cstate->line_buf);
	enlargeStringInfo(&cstate->line_buf, frame.line_len);
	if (CopyGetData(cstate, cstate->line_buf.data, frame.line_len) != frame.line_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	cstate->line_buf.data[frame.line_len] = '\0';
	cstate->line_buf.len = frame.line_len;
	cstate->line_buf.cursor = frame.residual_off;
	cstate->line_buf_valid = true;
	cstate->line_buf_converted = true;
	cstate->cur_lineno = frame.lineno;
	cstate->stopped_processing_at_delim = frame.delim_seen_at_end;

	/*
	 * Parse any fields from the input line that were not processed in the
	 * QD already.
	 */
	if (!cstate->cdbsreh)
	{
		if (!NextCopyFromX(cstate, econtext, values, nulls))
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));
		}
	}
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;
		bool		result;

		PG_TRY();
		{
			result = NextCopyFromX(cstate, econtext, values, nulls);

			if (!result)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("unexpected EOF in COPY data")));
		}
		PG_CATCH();
		{
			HandleCopyError(cstate);
			got_error = true;
			MemoryContextSwitchTo(oldcontext);
		}
		PG_END_TRY();
	}

	/*
	 * Read any attributes that were processed in the QD already. The attribute
	 * numbers in the message are already in terms of the target partition, so
	 * we do this after remapping and switching to the partition slot.
	 */
	for (i = 0; i < frame.fld_count; i++)
	{
		int16		attnum;
		int			m;
		int32		len;
		Datum		value;

		if (CopyGetData(cstate, &attnum, sizeof(attnum)) != sizeof(attnum))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));

		if (attnum < 1 || attnum > num_phys_attrs)
			elog(ERROR, "invalid attnum received from QD: %d (num physical attributes: %d)",
				 attnum, num_phys_attrs);
		m = attnum - 1;

		cstate->cur_attname = NameStr(attr[m].attname);

		if (attr[attnum - 1].attbyval)
		{
			if (CopyGetData(cstate, &value, sizeof(Datum)) != sizeof(Datum))
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("unexpected EOF in COPY data")));
		}
		else
		{
			char	   *p;

			if (attr[attnum - 1].attlen > 0)
			{
				len = attr[attnum - 1].attlen;

				p = palloc(len);
				if (CopyGetData(cstate, p, len) != len)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
			}
			else if (attr[attnum - 1].attlen == -1)
			{
				/* For simplicity, varlen's are always transmitted in "long" format */
				if (CopyGetData(cstate, &len, sizeof(len)) != sizeof(len))
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				if (len < VARHDRSZ)
					elog(ERROR, "invalid varlen length received from QD: %d", len);
				p = palloc(len);
				SET_VARSIZE(p, len);
				if (CopyGetData(cstate, p + VARHDRSZ, len - VARHDRSZ) != len - VARHDRSZ)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
			}
			else if (attr[attnum - 1].attlen == -2)
			{
				/*
				 * Like the varlen case above, cstrings are sent with a length
				 * prefix and no terminator, so we have to NULL-terminate in
				 * memory after reading them in.
				 */
				if (CopyGetData(cstate, &len, sizeof(len)) != sizeof(len))
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				p = palloc(len + 1);
				if (CopyGetData(cstate, p, len) != len)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				p[len] = '\0';
			}
			else
			{
				elog(ERROR, "attribute %d has invalid length %d",
					 attnum, attr[attnum - 1].attlen);
			}
			value = PointerGetDatum(p);
		}

		cstate->cur_attname = NULL;

		values[m] = value;
		nulls[m] = false;
	}

	if (got_error)
		goto retry;

	/*
	 * Here we should compute defaults for any columns for which we didn't
	 * get a default from the QD. But at the moment, all defaults are evaluated
	 * in the QD.
	 */
	return true;
}

/*
 * Parse and handle an "error frame" from QD.
 *
 * The caller has already read part of the frame; 'p' points to that part,
 * of length 'len'.
 */
static void
HandleQDErrorFrame(CopyFromState cstate, char *p, int len)
{
	CdbSreh *cdbsreh = cstate->cdbsreh;
	MemoryContext oldcontext;
	copy_from_dispatch_error errframe;
	char	   *errormsg;
	char	   *line;
	int			r;

	Assert(len <= SizeOfCopyFromDispatchError);

	Assert(Gp_role == GP_ROLE_EXECUTE);

	oldcontext = MemoryContextSwitchTo(cdbsreh->badrowcontext);

	/*
	 * Copy the part of the struct that the caller had already read, and
	 * read the rest.
	 */
	memcpy(&errframe, p, len);

	r = CopyGetData(cstate, ((char *) &errframe) + len, SizeOfCopyFromDispatchError - len);
	if (r != SizeOfCopyFromDispatchError - len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	errormsg = palloc(errframe.errmsg_len + 1);
	line = palloc(errframe.line_len + 1);

	r = CopyGetData(cstate, errormsg, errframe.errmsg_len);
	if (r != errframe.errmsg_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	errormsg[errframe.errmsg_len] = '\0';

	r = CopyGetData(cstate, line, errframe.line_len);
	if (r != errframe.line_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	line[errframe.line_len] = '\0';

	cdbsreh->linenumber = errframe.lineno;
	cdbsreh->rawdata->cursor = 0;
	cdbsreh->rawdata->data = line;

	/*
	 * GGDB: use the transmitted length, not strlen(): when the rejected
	 * line was an encoding error, the raw bytes may contain embedded NUL
	 * bytes, and strlen() would truncate the logged data at the first
	 * one.
	 */
	cdbsreh->rawdata->len = errframe.line_len;
	cdbsreh->errmsg = errormsg;
	cdbsreh->is_server_enc = errframe.line_buf_converted;

	HandleSingleRowError(cdbsreh);

	MemoryContextSwitchTo(oldcontext);

}

/*
 * Read the next input line and stash it in line_buf.
 *
 * Result is true if read was terminated by EOF, false if terminated
 * by newline.  The terminating newline or EOF marker is not included
 * in the final value of line_buf.
 */
static bool
CopyReadLine(CopyFromState cstate)
{
	bool		result;

	resetStringInfo(&cstate->line_buf);
	cstate->line_buf_valid = false;

	/*
	 * GGDB: mark that this line has not been (fully) converted to the server
	 * encoding yet.  If CopyReadLineText() bails out mid-line -- e.g. an
	 * encoding error, or an ERRCODE_BAD_COPY_FILE_FORMAT from the
	 * end-of-copy-marker checks, both of which SREH catches -- the rejected
	 * row must not be logged with a stale "converted" flag from the previous
	 * good line.  Set true again once the line is fully assembled below.
	 */
	cstate->line_buf_converted = false;

	/* Parse data and transfer into line_buf */
	result = CopyReadLineText(cstate);

	if (result)
	{
		/*
		 * Reached EOF.  In protocol version 3, we should ignore anything
		 * after \. up to the protocol end of copy data.  (XXX maybe better
		 * not to treat \. as special?)
		 */
		if (cstate->copy_src == COPY_FRONTEND)
		{
			int			inbytes;

			do
			{
				inbytes = CopyGetData(cstate, cstate->input_buf,
									  INPUT_BUF_SIZE);
			} while (inbytes > 0);
			cstate->input_buf_index = 0;
			cstate->input_buf_len = 0;
			cstate->raw_buf_index = 0;
			cstate->raw_buf_len = 0;
		}
	}
	else
	{
		/*
		 * If we didn't hit EOF, then we must have transferred the EOL marker
		 * to line_buf along with the data.  Get rid of it.
		 */
		switch (cstate->eol_type)
		{
			case EOL_NL:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_CR:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\r');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_CRNL:
				Assert(cstate->line_buf.len >= 2);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 2] == '\r');
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len -= 2;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_UNKNOWN:
				/* shouldn't get here */
				Assert(false);
				break;
		}
	}

	/* Now it's safe to use the buffer in error messages */
	cstate->line_buf_valid = true;

	/*
	 * GGDB: the data was already converted to server encoding, in
	 * CopyConvertBuf, before it entered input_buf and line_buf.  Mark
	 * line_buf as converted, for the benefit of the single-row error
	 * handling (is_server_enc) and the QD->QE error forwarding.
	 */
	cstate->line_buf_converted = true;

	return result;
}

/*
 * CopyReadLineText - inner loop of CopyReadLine for text mode
 */
static bool
CopyReadLineText(CopyFromState cstate)
{
	char	   *copy_input_buf;
	int			input_buf_ptr;
	int			copy_buf_len;
	bool		need_data = false;
	bool		hit_eof = false;
	bool		result = false;

	/* CSV variables */
	bool		first_char_in_line = true;
	bool		in_quote = false,
				last_was_esc = false;
	char		quotec = '\0';
	char		escapec = '\0';

	if (cstate->opts.csv_mode)
	{
		quotec = cstate->opts.quote[0];
		escapec = cstate->opts.escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';
	}

	/*
	 * The objective of this loop is to transfer the entire next input line
	 * into line_buf.  Hence, we only care for detecting newlines (\r and/or
	 * \n) and the end-of-copy marker (\.).
	 *
	 * In CSV mode, \r and \n inside a quoted field are just part of the data
	 * value and are put in line_buf.  We keep just enough state to know if we
	 * are currently in a quoted field or not.
	 *
	 * These four characters, and the CSV escape and quote characters, are
	 * assumed the same in frontend and backend encodings.
	 *
	 * The input has already been converted to the database encoding.  All
	 * supported server encodings have the property that all bytes in a
	 * multi-byte sequence have the high bit set, so a multibyte character
	 * cannot contain any newline or escape characters embedded in the
	 * multibyte sequence.  Therefore, we can process the input byte-by-byte,
	 * regardless of the encoding.
	 *
	 * For speed, we try to move data from input_buf to line_buf in chunks
	 * rather than one character at a time.  input_buf_ptr points to the next
	 * character to examine; any characters from input_buf_index to
	 * input_buf_ptr have been determined to be part of the line, but not yet
	 * transferred to line_buf.
	 *
	 * For a little extra speed within the loop, we copy input_buf and
	 * input_buf_len into local variables.
	 */
	copy_input_buf = cstate->input_buf;
	input_buf_ptr = cstate->input_buf_index;
	copy_buf_len = cstate->input_buf_len;

	for (;;)
	{
		int			prev_raw_ptr;
		char		c;

		/*
		 * Load more data if needed.  Ideally we would just force four bytes
		 * of read-ahead and avoid the many calls to
		 * IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(), but it hardly seems worth
		 * it, considering the size of the buffer.
		 */
		if (input_buf_ptr >= copy_buf_len || need_data)
		{
			REFILL_LINEBUF;

			CopyLoadInputBuf(cstate);
			/* update our local variables */
			hit_eof = cstate->input_reached_eof;
			input_buf_ptr = cstate->input_buf_index;
			copy_buf_len = cstate->input_buf_len;

			/*
			 * If we are completely out of data, break out of the loop,
			 * reporting EOF.
			 */
			if (INPUT_BUF_BYTES(cstate) <= 0)
			{
				result = true;
				break;
			}
			need_data = false;
		}

		/* OK to fetch a character */
		prev_raw_ptr = input_buf_ptr;
		c = copy_input_buf[input_buf_ptr++];

		if (cstate->opts.csv_mode)
		{
			/*
			 * If character is '\\' or '\r', we may need to look ahead below.
			 * Force fetch of the next character if we don't already have it.
			 * We need to do this before changing CSV state, in case one of
			 * these characters is also the quote or escape character.
			 *
			 * Note: old-protocol does not like forced prefetch, but it's OK
			 * here since we cannot validly be at EOF.
			 */
			if (c == '\\' || c == '\r')
			{
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			}

			/*
			 * Dealing with quotes and escapes here is mildly tricky. If the
			 * quote char is also the escape char, there's no problem - we
			 * just use the char as a toggle. If they are different, we need
			 * to ensure that we only take account of an escape inside a
			 * quoted field and immediately preceding a quote char, and not
			 * the second in an escape-escape sequence.
			 *
			 * GGDB: kept in CopyCSVQuoteStep() so CopyConsumeBadInputLine()
			 * can replay the exact same state transitions.
			 */
			CopyCSVQuoteStep(c, quotec, escapec, &in_quote, &last_was_esc);

			/*
			 * Updating the line count for embedded CR and/or LF chars is
			 * necessarily a little fragile - this test is probably about the
			 * best we can do.  (XXX it's arguable whether we should do this
			 * at all --- is cur_lineno a physical or logical count?)
			 */
			if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}

		/* Process \r */
		if (c == '\r' && (!cstate->opts.csv_mode || !in_quote))
		{
			/* Check for \r\n on first line, _and_ handle \r\n. */
			if (cstate->eol_type == EOL_UNKNOWN ||
				cstate->eol_type == EOL_CRNL)
			{
				/*
				 * If need more data, go back to loop top to load it.
				 *
				 * Note that if we are at EOF, c will wind up as '\0' because
				 * of the guaranteed pad of input_buf.
				 */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);

				/* get next char */
				c = copy_input_buf[input_buf_ptr];

				if (c == '\n')
				{
					input_buf_ptr++;	/* eat newline */
					cstate->eol_type = EOL_CRNL;	/* in case not set yet */

					/*
					 * GPDB: end of line. Since we don't error out if we find a
					 * bare CR or LF in CRLF mode, break here instead.
					 */
					break;
				}
				else
				{
					/*
					 * GPDB_91_MERGE_FIXME: these commented-out blocks (as well
					 * as the restructured newline checks) are here because we
					 * allow the user to manually set the newline mode, and
					 * therefore don't error out on bare CR/LF in the middle of
					 * a column. Instead, they will be included verbatim.
					 *
					 * This probably has other fallout -- but so does changing
					 * the behavior. Discuss.
					 */
#if 0
					/* found \r, but no \n */
					if (cstate->eol_type == EOL_CRNL)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 !cstate->opts.csv_mode ?
								 errmsg("literal carriage return found in data") :
								 errmsg("unquoted carriage return found in data"),
								 !cstate->opts.csv_mode ?
								 errhint("Use \"\\r\" to represent carriage return.") :
								 errhint("Use quoted CSV field to represent carriage return.")));
#endif

					/* GPDB: only reset eol_type if it's currently unknown. */
					if (cstate->eol_type == EOL_UNKNOWN)
					{
						/*
						 * if we got here, it is the first line and we didn't find
						 * \n, so don't consume the peeked character
						 */
						cstate->eol_type = EOL_CR;
					}
				}
			}
#if 0 /* GPDB_91_MERGE_FIXME: see above. */
			else if (cstate->eol_type == EOL_NL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->opts.csv_mode ?
						 errmsg("literal carriage return found in data") :
						 errmsg("unquoted carriage return found in data"),
						 !cstate->opts.csv_mode ?
						 errhint("Use \"\\r\" to represent carriage return.") :
						 errhint("Use quoted CSV field to represent carriage return.")));
#endif
			/* GPDB: a CR only ends the line in CR mode. */
			if (cstate->eol_type == EOL_CR)
			{
				/* If reach here, we have found the line terminator */
				break;
			}
		}

		/* Process \n */
		if (c == '\n' && (!cstate->opts.csv_mode || !in_quote))
		{
#if 0 /* GPDB_91_MERGE_FIXME: see above. */
			if (cstate->eol_type == EOL_CR || cstate->eol_type == EOL_CRNL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->opts.csv_mode ?
						 errmsg("literal newline found in data") :
						 errmsg("unquoted newline found in data"),
						 !cstate->opts.csv_mode ?
						 errhint("Use \"\\n\" to represent newline.") :
						 errhint("Use quoted CSV field to represent newline.")));
#endif
			/* GPDB: only reset eol_type if it's currently unknown. */
			if (cstate->eol_type == EOL_UNKNOWN)
				cstate->eol_type = EOL_NL;	/* in case not set yet */

			/* GPDB: a LF only ends the line in LF mode. */
			if (cstate->eol_type == EOL_NL)
			{
				/* If reach here, we have found the line terminator */
				break;
			}
		}

		/*
		 * In CSV mode, we only recognize \. alone on a line.  This is because
		 * \. is a valid CSV data value.
		 */
		if (c == '\\' && (!cstate->opts.csv_mode || first_char_in_line))
		{
			char		c2;

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			IF_NEED_REFILL_AND_EOF_BREAK(0);

			/* -----
			 * get next character
			 * Note: we do not change c so if it isn't \., we can fall
			 * through and continue processing.
			 * -----
			 */
			c2 = copy_input_buf[input_buf_ptr];

			/*
			 * We need to recognize the EOL.
			 * Github issue: https://github.com/greenplum-db/gpdb/issues/12454
			 */
			if(c2 == '\n')
			{
				if(cstate->eol_type == EOL_UNKNOWN)
				{
				/* We still do not found the first EOL.
				 * The current '\n' will be recongnized as EOL
				 * in next loop of c1.
				 */
					goto not_end_of_copy;
				}
				else if(cstate->eol_type == EOL_NL)
				{
					// found a new line with '\n'
					input_buf_ptr++;
					break;
				}
			}
			if (c2 == '\r')
			{
				if(cstate->eol_type == EOL_UNKNOWN)
				{
					goto not_end_of_copy;
				}
				else if(cstate->eol_type == EOL_CR)
				{
					// found a new line wirh '\r'
					input_buf_ptr++;
					break;
				}
				else if(cstate->eol_type == EOL_CRNL)
				{
					/*
					 * Because the eol is '\r\n', we need another character c3
					 * which comes after c2 if exists.
					 */
					char c3;
					input_buf_ptr++;
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					IF_NEED_REFILL_AND_EOF_BREAK(0);
					c3 = copy_input_buf[input_buf_ptr];
					if(c3 == '\n')
					{
						// found a new line with '\r\n'
						input_buf_ptr++;
						break;
					} else {
						NO_END_OF_COPY_GOTO;
					}
				}
			}
			if (c2 == '.')
			{
				input_buf_ptr++;	/* consume the '.' */

				/*
				 * Note: if we loop back for more data here, it does not
				 * matter that the CSV state change checks are re-executed; we
				 * will come back here with no important state changed.
				 */
				if (cstate->eol_type == EOL_CRNL)
				{
					/* Get the next character */
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					/* if hit_eof, c2 will become '\0' */
					c2 = copy_input_buf[input_buf_ptr++];

					if (c2 == '\n')
					{
						if (!cstate->opts.csv_mode)
						{
							cstate->input_buf_index = input_buf_ptr;
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker does not match previous newline style")));
						}
						else
							NO_END_OF_COPY_GOTO;
					}
					else if (c2 != '\r')
					{
						if (!cstate->opts.csv_mode)
						{
							cstate->input_buf_index = input_buf_ptr;
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker corrupt")));
						}
						else
							NO_END_OF_COPY_GOTO;
					}
				}

				/* Get the next character */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
				/* if hit_eof, c2 will become '\0' */
				c2 = copy_input_buf[input_buf_ptr++];

				if (c2 != '\r' && c2 != '\n')
				{
					if (!cstate->opts.csv_mode)
					{
						cstate->input_buf_index = input_buf_ptr;
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 errmsg("end-of-copy marker corrupt")));
					}
					else
						NO_END_OF_COPY_GOTO;
				}

				if ((cstate->eol_type == EOL_NL && c2 != '\n') ||
					(cstate->eol_type == EOL_CRNL && c2 != '\n') ||
					(cstate->eol_type == EOL_CR && c2 != '\r'))
				{
					cstate->input_buf_index = input_buf_ptr;
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker does not match previous newline style")));
				}

				/*
				 * Transfer only the data before the \. into line_buf, then
				 * discard the data and the \. sequence.
				 */
				if (prev_raw_ptr > cstate->input_buf_index)
					appendBinaryStringInfo(&cstate->line_buf,
										   cstate->input_buf + cstate->input_buf_index,
										   prev_raw_ptr - cstate->input_buf_index);
				cstate->input_buf_index = input_buf_ptr;
				result = true;	/* report EOF */
				break;
			}
			else if (!cstate->opts.csv_mode)
			{
				/*
				 * If we are here, it means we found a backslash followed by
				 * something other than a period.  In non-CSV mode, anything
				 * after a backslash is special, so we skip over that second
				 * character too.  If we didn't do that \\. would be
				 * considered an eof-of copy, while in non-CSV mode it is a
				 * literal backslash followed by a period.  In CSV mode,
				 * backslashes are not special, so we want to process the
				 * character after the backslash just like a normal character,
				 * so we don't increment in those cases.
				 */
				input_buf_ptr++;
			}
		}

		/*
		 * This label is for CSV cases where \. appears at the start of a
		 * line, but there is more text after it, meaning it was a data value.
		 * We are more strict for \. in CSV mode because \. could be a data
		 * value, while in non-CSV mode, \. cannot be a data value.
		 */
not_end_of_copy:
		first_char_in_line = false;
	}							/* end of outer loop */

	/*
	 * Transfer any still-uncopied data to line_buf.
	 */
	REFILL_LINEBUF;

	return result;
}

/*
 *	Return decimal value for a hexadecimal digit
 */
static int
GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char) hex))
		return hex - '0';
	else
		return tolower((unsigned char) hex) - 'a' + 10;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.
 *
 * The input is in line_buf.  We use attribute_buf to hold the result
 * strings.  cstate->raw_fields[k] is set to point to the k'th attribute
 * string, or NULL when the input matches the null marker string.
 * This array is expanded as necessary.
 *
 * (Note that the caller cannot check for nulls since the returned
 * string would be the post-de-escaping equivalent, which may look
 * the same as some valid data string.)
 *
 * delim is the column delimiter string (must be just one byte for now).
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string.
 *
 * The return value is the number of fields actually read.
 */
static int
CopyReadAttributesText(CopyFromState cstate, int stop_processing_at_field)
{
	char		delimc = cstate->opts.delim[0];
	char		escapec = cstate->opts.escape[0];
	bool		delim_off = cstate->opts.delim_off;
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data + cstate->line_buf.cursor;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;
		bool		saw_non_ascii = false;

		/*
		 * In QD, stop once we have processed the last field we need in the QD.
		 */
		if (fieldno == stop_processing_at_field)
		{
			cstate->stopped_processing_at_delim = true;
			break;
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field.
		 *
		 * Note that in this loop, we are scanning to locate the end of field
		 * and also speculatively performing de-escaping.  Once we find the
		 * end-of-field, we can match the raw field contents against the null
		 * marker string.  Only after that comparison fails do we know that
		 * de-escaping is actually the right thing to do; therefore we *must
		 * not* throw any syntax errors before we've done the null-marker
		 * check.
		 */
		for (;;)
		{
			char		c;

			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (c == delimc && !delim_off)
			{
				found_delim = true;
				break;
			}
			if (c == escapec && !cstate->opts.escape_off)
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							/* handle \013 */
							int			val;

							val = OCTVALUE(c);
							if (cur_ptr < line_end_ptr)
							{
								c = *cur_ptr;
								if (ISOCTAL(c))
								{
									cur_ptr++;
									val = (val << 3) + OCTVALUE(c);
									if (cur_ptr < line_end_ptr)
									{
										c = *cur_ptr;
										if (ISOCTAL(c))
										{
											cur_ptr++;
											val = (val << 3) + OCTVALUE(c);
										}
									}
								}
							}
							c = val & 0377;
							if (c == '\0' || IS_HIGHBIT_SET(c))
								saw_non_ascii = true;
						}
						break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char		hexchar = *cur_ptr;

							if (isxdigit((unsigned char) hexchar))
							{
								int			val = GetDecimalFromHex(hexchar);

								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char) hexchar))
									{
										cur_ptr++;
										val = (val << 4) + GetDecimalFromHex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;

						/*
						 * in all other cases, take the char after '\'
						 * literally
						 */
				}
			}

			/* Add c to output string */
			*output_ptr++ = c;
		}

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cstate->opts.null_print_len &&
			strncmp(start_ptr, cstate->opts.null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;
		else
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 */
			if (saw_non_ascii)
			{
				char	   *fld = cstate->raw_fields[fieldno];

				pg_verifymbstr(fld, output_ptr - fld, false);
			}
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
		{
			cstate->stopped_processing_at_delim = false;
			break;
		}
	}

	/*
	 * Make note of the stopping point in 'line_buf.cursor', so that we
	 * can send the rest to the QE later.
	 */
	cstate->line_buf.cursor = cur_ptr - cstate->line_buf.data;

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.  This has exactly the same API as
 * CopyReadAttributesText, except we parse the fields according to
 * "standard" (i.e. common) CSV usage.
 */
static int
CopyReadAttributesCSV(CopyFromState cstate, int stop_processing_at_field)
{
	char		delimc = cstate->opts.delim[0];
	bool		delim_off = cstate->opts.delim_off;
	char		quotec = cstate->opts.quote[0];
	char		escapec = cstate->opts.escape[0];
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data + cstate->line_buf.cursor;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		bool		saw_quote = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;

		/*
		 * In QD, stop once we have processed the last field we need in the QD.
		 */
		if (fieldno == stop_processing_at_field)
		{
			cstate->stopped_processing_at_delim = true;
			break;
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field,
		 *
		 * The loop starts in "not quote" mode and then toggles between that
		 * and "in quote" mode. The loop exits normally if it is in "not
		 * quote" mode and a delimiter or line end is seen.
		 */
		for (;;)
		{
			char		c;

			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if (c == delimc && !delim_off)
				{
					found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}

			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unterminated CSV quoted field")));

				c = *cur_ptr++;

				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char		nextc = *cur_ptr;

						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}

				/*
				 * end of quoted field. Must do this test after testing for
				 * escape in case quote char and escape char are the same
				 * (which is the common case).
				 */
				if (c == quotec)
					break;

				/* Add c to output string */
				*output_ptr++ = c;
			}
		}
endfield:

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == cstate->opts.null_print_len &&
			strncmp(start_ptr, cstate->opts.null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
		{
			cstate->stopped_processing_at_delim = false;
			break;
		}
	}

	/*
	 * Make note of the stopping point in 'line_buf.cursor', so that we
	 * can send the rest to the QE later.
	 */
	cstate->line_buf.cursor = cur_ptr - cstate->line_buf.data;

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}


/*
 * Read a binary attribute
 */
static Datum
CopyReadBinaryAttribute(CopyFromState cstate, FmgrInfo *flinfo,
						Oid typioparam, int32 typmod,
						bool *isnull)
{
	int32		fld_size;
	Datum		result;

	if (!CopyGetInt32(cstate, &fld_size))
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	if (fld_size == -1)
	{
		*isnull = true;
		return ReceiveFunctionCall(flinfo, NULL, typioparam, typmod);
	}
	if (fld_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid field size")));

	/* reset attribute_buf to empty, and load raw data in it */
	resetStringInfo(&cstate->attribute_buf);

	enlargeStringInfo(&cstate->attribute_buf, fld_size);
	if (CopyReadBinaryData(cstate, cstate->attribute_buf.data,
						   fld_size) != fld_size)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	cstate->attribute_buf.len = fld_size;
	cstate->attribute_buf.data[fld_size] = '\0';

	/* Call the column type's binary input converter */
	result = ReceiveFunctionCall(flinfo, &cstate->attribute_buf,
								 typioparam, typmod);

	/* Trouble if it didn't eat the whole buffer */
	if (cstate->attribute_buf.cursor != cstate->attribute_buf.len)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("incorrect binary data format")));

	*isnull = false;
	return result;
}
