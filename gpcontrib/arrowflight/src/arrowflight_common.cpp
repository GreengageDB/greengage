/*-------------------------------------------------------------------------
 *
 * arrowflight_common.cpp
 *	  URL and option validation helpers for the Arrow Flight extension.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "catalog/pg_type.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
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
static int	af_hex_value(char ch);
static Size af_decode_url_component(const char *src, Size srclen, char *dst,
									Size dstlen,
									const char *component_name);
static bool af_dataset_char_allowed(unsigned char ch);
static bool af_operation_metadata_key_char_allowed(unsigned char ch);
static bool af_operation_metadata_value_char_allowed(unsigned char ch);
static void af_append_expanded_ticket_part(StringInfo out, const char *part,
										   Size part_len);

bool
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
af_validate_fdw_write_mode(const char *write_mode)
{
	if (write_mode != NULL &&
		(strcmp(write_mode, AF_FDW_WRITE_MODE_STAGING) == 0 ||
		 strcmp(write_mode, AF_FDW_WRITE_MODE_APPEND) == 0))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
			 errmsg("invalid arrowflight_fdw write_mode \"%s\"",
					write_mode ? write_mode : ""),
			 errhint("Use \"%s\" or \"%s\".",
					 AF_FDW_WRITE_MODE_STAGING, AF_FDW_WRITE_MODE_APPEND)));
}

void
af_validate_fdw_dataset(const char *dataset)
{
	Size		len;
	bool		prev_slash = true;

	if (dataset == NULL || dataset[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw dataset must not be empty")));

	len = strlen(dataset);
	if (len > AF_MAX_DATASET_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw dataset is too long"),
				 errhint("Use at most %d bytes.", AF_MAX_DATASET_LEN)));

	if (strstr(dataset, "..") != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw dataset must not contain \"..\"")));

	for (Size i = 0; i < len; i++)
	{
		unsigned char ch = (unsigned char) dataset[i];

		if (!af_dataset_char_allowed(ch))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("unsafe character in arrowflight_fdw dataset"),
					 errhint("Use only letters, digits, '.', '_', '-', and '/'.")));

		if (ch == '/')
		{
			if (prev_slash)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("arrowflight_fdw dataset contains an empty path component")));
			prev_slash = true;
		}
		else
			prev_slash = false;
	}

	if (prev_slash)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw dataset contains an empty path component")));
}

void
af_validate_fdw_operation_metadata(const char *metadata)
{
	const char *pos;

	if (metadata == NULL || metadata[0] == '\0')
		return;

	if (strlen(metadata) > AF_MAX_OPERATION_METADATA_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw operation_metadata is too long"),
				 errhint("Use at most %d bytes.",
						 AF_MAX_OPERATION_METADATA_LEN)));

	pos = metadata;
	while (*pos != '\0')
	{
		const char *entry_end = pos;
		const char *eq;
		Size		key_len;
		Size		static_prefix_len = strlen("static.");
		Size		af_static_prefix_len = strlen("af.static.");

		while (*entry_end != '\0' && *entry_end != ',' &&
			   *entry_end != ';')
			entry_end++;

		eq = (const char *) memchr(pos, '=', entry_end - pos);
		if (eq == NULL || eq == pos || eq + 1 == entry_end)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("invalid arrowflight_fdw operation_metadata entry"),
					 errhint("Use comma-separated key=value pairs.")));

		key_len = eq - pos;
		if (!((key_len > static_prefix_len &&
			   strncmp(pos, "static.", static_prefix_len) == 0) ||
			  (key_len > af_static_prefix_len &&
			   strncmp(pos, "af.static.", af_static_prefix_len) == 0)))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("invalid arrowflight_fdw operation_metadata key"),
					 errhint("Use keys in the static.* or af.static.* namespace.")));

		for (const char *key = pos; key < eq; key++)
		{
			if (!af_operation_metadata_key_char_allowed((unsigned char) *key))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("unsafe character in arrowflight_fdw operation_metadata key")));
		}

		for (const char *value = eq + 1; value < entry_end; value++)
		{
			if (!af_operation_metadata_value_char_allowed((unsigned char) *value))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("unsafe character in arrowflight_fdw operation_metadata value")));
		}

		pos = entry_end;
		if (*pos == ',' || *pos == ';')
		{
			pos++;
			if (*pos == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid arrowflight_fdw operation_metadata entry"),
						 errhint("Use comma-separated key=value pairs.")));
		}
	}
}

void
af_validate_endpoint_policy(const char *policy)
{
	if (policy == NULL ||
		strcmp(policy, AF_ENDPOINT_POLICY_FIRST) == 0 ||
		strcmp(policy, AF_ENDPOINT_POLICY_SEGMENT_INDEX) == 0)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid flight_endpoint_policy value \"%s\"", policy),
			 errhint("Use \"%s\" or \"%s\".",
					 AF_ENDPOINT_POLICY_FIRST,
					 AF_ENDPOINT_POLICY_SEGMENT_INDEX)));
}

void
af_validate_projection_pushdown(const char *mode)
{
	if (mode != NULL &&
		(strcmp(mode, AF_PROJECTION_PUSHDOWN_OFF) == 0 ||
		 strcmp(mode, AF_PROJECTION_PUSHDOWN_TRY) == 0 ||
		 strcmp(mode, AF_PROJECTION_PUSHDOWN_REQUIRE) == 0))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid projection_pushdown value \"%s\"",
					mode ? mode : ""),
			 errhint("Use \"%s\", \"%s\", or \"%s\".",
					 AF_PROJECTION_PUSHDOWN_OFF,
					 AF_PROJECTION_PUSHDOWN_TRY,
					 AF_PROJECTION_PUSHDOWN_REQUIRE)));
}

void
af_parse_flight_endpoint(const char *url, ArrowFlightEndpoint *endpoint)
{
	const char *pos;
	const char *host_start;
	const char *host_end;
	const char *port_start;
	const char *path_start = NULL;
	const char *query_start = NULL;
	Size		host_len;
	Size		ticket_len;
	Size		decoded_ticket_len;
	char		port_buf[16];
	char	   *endptr;
	long		port;

	if (url == NULL || !af_has_scheme(url))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight URL \"%s\"", url ? url : "")));

	memset(endpoint, 0, sizeof(*endpoint));
	endpoint->port = -1;
	endpoint->tls = af_get_url_bool_option(url, "tls", false);

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

	memcpy(endpoint->host, host_start, host_len);
	endpoint->host[host_len] = '\0';

	if (*pos != ':')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Arrow Flight URL must include an explicit port"),
				 errhint("Use arrowflight://host:port/ticket.")));

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

	endpoint->port = (int) port;

	if (*pos == '/')
	{
		path_start = ++pos;
		while (*pos != '\0' && *pos != '?')
			pos++;
	}

	if (*pos == '?')
		query_start = pos + 1;

	if (af_get_url_option(url, "ticket", endpoint->ticket,
						  sizeof(endpoint->ticket)))
	{
		if (endpoint->ticket[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Arrow Flight URL option \"ticket\" must not be empty")));
		return;
	}

	if (path_start == NULL || path_start == pos)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Arrow Flight URL must include a ticket path or ticket option"),
				 errhint("Use arrowflight://host:port/ticket or arrowflight://host:port/?ticket=value.")));

	ticket_len = (query_start == NULL ? strlen(path_start) : (Size) (pos - path_start));
	if (ticket_len == 0 || ticket_len > AF_MAX_TICKET_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight ticket in URL \"%s\"", url)));

	decoded_ticket_len = af_decode_url_component(path_start, ticket_len,
												 endpoint->ticket,
												 sizeof(endpoint->ticket),
												 "ticket path");
	if (decoded_ticket_len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Arrow Flight ticket in URL \"%s\"", url)));
}

void
af_expand_ticket_placeholders(ArrowFlightEndpoint *endpoint)
{
	StringInfoData expanded;
	const char *pos;

	initStringInfo(&expanded);
	pos = endpoint->ticket;

	while (*pos != '\0')
	{
		if (strncmp(pos, AF_TICKET_SEGID_PLACEHOLDER,
					sizeof(AF_TICKET_SEGID_PLACEHOLDER) - 1) == 0)
		{
			appendStringInfo(&expanded, "%d", GpIdentity.segindex);
			pos += sizeof(AF_TICKET_SEGID_PLACEHOLDER) - 1;
			continue;
		}

		if (strncmp(pos, AF_TICKET_SEGCOUNT_PLACEHOLDER,
					sizeof(AF_TICKET_SEGCOUNT_PLACEHOLDER) - 1) == 0)
		{
			appendStringInfo(&expanded, "%d", getgpsegmentCount());
			pos += sizeof(AF_TICKET_SEGCOUNT_PLACEHOLDER) - 1;
			continue;
		}

		af_append_expanded_ticket_part(&expanded, pos, 1);
		pos++;
	}

	if (expanded.len == 0 || expanded.len > AF_MAX_TICKET_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expanded Arrow Flight ticket is invalid or too long")));

	memcpy(endpoint->ticket, expanded.data, expanded.len + 1);
	pfree(expanded.data);
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

static bool
af_dataset_char_allowed(unsigned char ch)
{
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '.' || ch == '_' || ch == '-' || ch == '/';
}

static bool
af_operation_metadata_key_char_allowed(unsigned char ch)
{
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '.' || ch == '_' || ch == '-';
}

static bool
af_operation_metadata_value_char_allowed(unsigned char ch)
{
	return ch >= 0x20 && ch != 0x7f && ch != ',' && ch != ';';
}

static void
af_append_expanded_ticket_part(StringInfo out, const char *part, Size part_len)
{
	appendBinaryStringInfo(out, part, part_len);
}
