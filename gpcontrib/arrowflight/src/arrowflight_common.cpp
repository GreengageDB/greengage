/*-------------------------------------------------------------------------
 *
 * arrowflight_common.cpp
 *	  URL, option, and type helpers for the Flight SQL extension.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

}

#include <errno.h>
#include <limits.h>
#include <string.h>

static bool af_raw_url_char_allowed(unsigned char ch);
static bool af_has_scheme(const char *url);
static int	af_hex_value(char ch);
static Size af_decode_url_component(const char *src, Size srclen, char *dst,
									Size dstlen,
									const char *component_name);

static bool
af_has_scheme(const char *url)
{
	return url != NULL && pg_strncasecmp(url, AF_SCHEME, AF_SCHEME_LEN) == 0;
}

bool
af_get_url_option(const char *url, const char *key, char *dst, Size dstlen)
{
	const char *query;
	Size		keylen;

	if (url == NULL || key == NULL || dst == NULL || dstlen == 0)
		return false;

	query = strchr(url, '?');
	if (query == NULL)
		return false;

	query++;
	keylen = strlen(key);

	while (*query != '\0')
	{
		const char *part_end;
		const char *eq;

		while (*query == '&')
			query++;

		part_end = strchr(query, '&');
		if (part_end == NULL)
			part_end = query + strlen(query);

		eq = (const char *) memchr(query, '=', part_end - query);
		if (eq != NULL &&
			(Size) (eq - query) == keylen &&
			strncmp(query, key, keylen) == 0)
		{
			Size		vallen = part_end - eq - 1;

			(void) af_decode_url_component(eq + 1, vallen, dst, dstlen, key);
			return true;
		}

		query = part_end;
	}

	return false;
}

bool
af_get_url_bool_option(const char *url, const char *key, bool default_value)
{
	char		value[16];

	if (!af_get_url_option(url, key, value, sizeof(value)))
		return default_value;

	if (pg_strcasecmp(value, "true") == 0 ||
		pg_strcasecmp(value, "on") == 0 ||
		pg_strcasecmp(value, "yes") == 0 ||
		strcmp(value, "1") == 0)
		return true;

	if (pg_strcasecmp(value, "false") == 0 ||
		pg_strcasecmp(value, "off") == 0 ||
		pg_strcasecmp(value, "no") == 0 ||
		strcmp(value, "0") == 0)
		return false;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid boolean value \"%s\" for Arrow Flight URL option \"%s\"",
					value, key)));

	return default_value;
}

int
af_parse_int_option_value(const char *value, const char *key, int min_value,
						  int max_value)
{
	char	   *endptr;
	long		parsed;

	if (value == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid %s value", key)));

	errno = 0;
	parsed = strtol(value, &endptr, 10);
	if (errno != 0 || endptr == value || *endptr != '\0' ||
		parsed < min_value || parsed > max_value)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid %s value \"%s\"", key, value),
				 errhint("Use an integer value between %d and %d.",
						 min_value, max_value)));

	return (int) parsed;
}

int
af_get_url_int_option(const char *url, const char *key, int default_value,
					  int min_value, int max_value)
{
	char		value[32];

	if (!af_get_url_option(url, key, value, sizeof(value)))
		return default_value;

	return af_parse_int_option_value(value, key, min_value, max_value);
}

void
af_parse_flight_connection(const char *url,
						   ArrowFlightConnection *connection)
{
	const char *pos;
	const char *host_start;
	const char *host_end;
	const char *port_start;
	Size		host_len;
	char		port_buf[16];
	char	   *endptr;
	long		port;

	if (url == NULL || !af_has_scheme(url))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight URL \"%s\"", url ? url : "")));

	memset(connection, 0, sizeof(*connection));
	connection->port = -1;
	connection->tls = af_get_url_bool_option(url, "tls", false);

	pos = url + AF_SCHEME_LEN;
	host_start = pos;

	if (*pos == '[')
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("IPv6 Arrow Flight URLs are not supported yet")));

	while (*pos != '\0' && *pos != ':' && *pos != '/' && *pos != '?')
		pos++;

	host_end = pos;
	host_len = host_end - host_start;
	if (host_len == 0 || host_len > AF_MAX_HOST_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight host in URL \"%s\"", url)));

	memcpy(connection->host, host_start, host_len);
	connection->host[host_len] = '\0';

	if (*pos != ':')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Arrow Flight URL must include an explicit port"),
				 errhint("Use arrowflight://host:port/flightsql.")));

	port_start = ++pos;
	while (*pos >= '0' && *pos <= '9')
		pos++;

	if (pos == port_start || (Size) (pos - port_start) >= sizeof(port_buf))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight port in URL \"%s\"", url)));

	memcpy(port_buf, port_start, pos - port_start);
	port_buf[pos - port_start] = '\0';

	errno = 0;
	port = strtol(port_buf, &endptr, 10);
	if (errno != 0 || endptr == port_buf || *endptr != '\0' ||
		port <= 0 || port > 65535)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight port \"%s\"", port_buf)));

	if (*pos != '\0' && *pos != '/' && *pos != '?')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight URL \"%s\"", url)));

	connection->port = (int) port;
}

bool
af_type_uses_text_exchange(Oid typid)
{
	switch (typid)
	{
		case JSONOID:
		case JSONBOID:
			return true;
		default:
			break;
	}

	return af_type_is_enum(typid) || OidIsValid(get_element_type(typid));
}

bool
af_type_is_enum(Oid typid)
{
	return get_typtype(typid) == TYPTYPE_ENUM;
}

Datum
af_input_text_datum(Form_pg_attribute attr, const char *data, int32 len)
{
	Oid			typinput;
	Oid			typioparam;
	FmgrInfo	input_finfo;
	char	   *value;
	Datum		result;

	getTypeInputInfo(attr->atttypid, &typinput, &typioparam);
	fmgr_info(typinput, &input_finfo);

	value = pnstrdup(data, len);
	result = InputFunctionCall(&input_finfo, value, typioparam,
							   attr->atttypmod);
	pfree(value);

	return result;
}

Datum
af_input_varchar_datum(Form_pg_attribute attr, const char *data, int32 len)
{
	int32		out_len = len;

	if (attr->atttypid != VARCHAROID)
		elog(ERROR, "af_input_varchar_datum called for non-varchar type");

	if (len < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid varchar payload length %d", len)));

	if (attr->atttypmod >= (int32) VARHDRSZ)
	{
		int32		maxlen = attr->atttypmod - VARHDRSZ;

		if (len > maxlen)
		{
			int			mbmaxlen = pg_mbcharcliplen(data, len, maxlen);

			for (int j = mbmaxlen; j < len; j++)
			{
				if (data[j] != ' ')
					ereport(ERROR,
							(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
							 errmsg("value too long for type character varying(%d)",
									maxlen)));
			}

			out_len = mbmaxlen;
		}
	}

	return PointerGetDatum(cstring_to_text_with_len(data, out_len));
}

char *
af_output_text_datum(Oid typid, Datum value)
{
	Oid			typoutput;
	bool		typisvarlena;
	FmgrInfo	output_finfo;

	getTypeOutputInfo(typid, &typoutput, &typisvarlena);
	fmgr_info(typoutput, &output_finfo);

	return OutputFunctionCall(&output_finfo, value);
}

Datum
af_uuid_datum_from_bytes(const unsigned char *data, Size len)
{
	pg_uuid_t  *uuid;

	if (len != UUID_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid uuid payload length %zu", len)));

	uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));
	memcpy(uuid->data, data, UUID_LEN);
	return UUIDPGetDatum(uuid);
}

Datum
af_interval_datum_from_parts(int32 months, int32 days, int64 time_usecs)
{
	Interval   *interval = (Interval *) palloc(sizeof(Interval));

	interval->month = months;
	interval->day = days;
	interval->time = time_usecs;
	return IntervalPGetDatum(interval);
}

static bool
af_raw_url_char_allowed(unsigned char ch)
{
	if ((ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9'))
		return true;

	switch (ch)
	{
		case '-':
		case '.':
		case '_':
		case '~':
		case '/':
		case ':':
		case '@':
		case '!':
		case '$':
		case '&':
		case '\'':
		case '(':
		case ')':
		case '*':
		case '+':
		case ',':
		case ';':
		case '=':
			return true;
		default:
			return false;
	}
}

static int
af_hex_value(char ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'A' && ch <= 'F')
		return ch - 'A' + 10;
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	return -1;
}

static Size
af_decode_url_component(const char *src, Size srclen, char *dst, Size dstlen,
						const char *component_name)
{
	Size		i;
	Size		outlen = 0;

	if (dstlen == 0)
		elog(ERROR, "invalid Arrow Flight URL decode buffer");

	for (i = 0; i < srclen; i++)
	{
		unsigned char ch = (unsigned char) src[i];

		if (ch == '%')
		{
			int			hi;
			int			lo;

			if (i + 2 >= srclen)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid percent escape in Arrow Flight URL %s",
								component_name)));

			hi = af_hex_value(src[i + 1]);
			lo = af_hex_value(src[i + 2]);
			if (hi < 0 || lo < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid percent escape in Arrow Flight URL %s",
								component_name)));

			ch = (unsigned char) ((hi << 4) | lo);
			i += 2;

			if (ch == '\0' || ch < 0x20 || ch == 0x7f)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid escaped byte in Arrow Flight URL %s",
								component_name)));
		}
		else if (!af_raw_url_char_allowed(ch))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsafe unescaped character in Arrow Flight URL %s",
							component_name),
					 errhint("Percent-encode reserved characters such as spaces, braces and percent signs.")));

		if (outlen + 1 >= dstlen)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Arrow Flight URL %s is too long", component_name)));

		dst[outlen++] = (char) ch;
	}

	dst[outlen] = '\0';
	return outlen;
}
