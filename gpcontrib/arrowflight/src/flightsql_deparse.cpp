/*-------------------------------------------------------------------------
 *
 * flightsql_deparse.cpp
 *    Conservative predicate deparser for Arrow Flight SQL.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include <math.h>

}

#define FLIGHTSQL_MAX_IN_VALUES 1000

typedef enum FlightSqlPredicateType
{
	FLIGHTSQL_PREDICATE_UNSUPPORTED,
	FLIGHTSQL_PREDICATE_BOOL,
	FLIGHTSQL_PREDICATE_INTEGER,
	FLIGHTSQL_PREDICATE_FLOAT,
	FLIGHTSQL_PREDICATE_NUMERIC,
	FLIGHTSQL_PREDICATE_STRING,
	FLIGHTSQL_PREDICATE_ENUM,
	FLIGHTSQL_PREDICATE_DATE,
	FLIGHTSQL_PREDICATE_TIME,
	FLIGHTSQL_PREDICATE_TIMESTAMP
} FlightSqlPredicateType;

typedef struct FlightSqlDeparseContext
{
	StringInfo	buf;
	Oid			foreigntableid;
	Index		scan_relid;
} FlightSqlDeparseContext;

static Node *flightsql_strip_relabel(Node *node);
static FlightSqlPredicateType flightsql_predicate_type(Oid typid);
static bool flightsql_string_is_safe(const char *value);
static bool flightsql_literal_is_safe(Oid typid, Datum value, bool is_null);
static bool flightsql_operator_info(Oid opno, char *name, Size name_size,
									Oid *function_id);
static bool flightsql_operator_is_comparison(const char *name);
static bool flightsql_operator_is_equality(const char *name);
static bool flightsql_comparison_is_safe(Oid left_type, Oid right_type,
										 const char *operator_name,
										 Oid input_collation);
static bool flightsql_var_is_safe(Node *node, Index scan_relid);
static bool flightsql_operand_is_safe(Node *node, Index scan_relid,
									  bool *has_var);
static bool flightsql_array_is_safe(Node *node, Oid scalar_type,
									Oid input_collation);
static bool flightsql_expression_is_safe(Node *node, Index scan_relid,
										 bool require_leakproof,
										 bool *has_var);
static char *flightsql_column_name(Oid foreigntableid, AttrNumber attnum);
static void flightsql_append_quoted_string(StringInfo buf, const char *value);
static void flightsql_append_date(StringInfo buf, DateADT value);
static void flightsql_append_time(StringInfo buf, TimeADT value);
static void flightsql_append_timestamp(StringInfo buf, Timestamp value);
static void flightsql_append_literal(StringInfo buf, Oid typid, Datum value,
									 bool is_null);
static void flightsql_deparse_expression(Node *node,
										 FlightSqlDeparseContext *context);
static void flightsql_deparse_array(Node *node,
									FlightSqlDeparseContext *context);

bool
af_flightsql_predicate_is_safe(Expr *expression, Index scan_relid,
							   bool require_leakproof)
{
	bool		has_var = false;

	if (!flightsql_expression_is_safe((Node *) expression, scan_relid,
									  require_leakproof, &has_var))
		return false;
	return has_var;
}

void
af_flightsql_append_predicates(StringInfo buf, Oid foreigntableid,
							   Index scan_relid, List *expressions)
{
	FlightSqlDeparseContext context;
	ListCell   *lc;
	bool		first = true;

	if (expressions == NIL)
		return;

	context.buf = buf;
	context.foreigntableid = foreigntableid;
	context.scan_relid = scan_relid;
	appendStringInfoString(buf, " WHERE ");

	foreach(lc, expressions)
	{
		if (!first)
			appendStringInfoString(buf, " AND ");
		flightsql_deparse_expression((Node *) lfirst(lc), &context);
		first = false;
	}
}

static Node *
flightsql_strip_relabel(Node *node)
{
	while (node != NULL && IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;
	return node;
}

static FlightSqlPredicateType
flightsql_predicate_type(Oid typid)
{
	switch (typid)
	{
		case BOOLOID:
			return FLIGHTSQL_PREDICATE_BOOL;
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return FLIGHTSQL_PREDICATE_INTEGER;
		case FLOAT4OID:
		case FLOAT8OID:
			return FLIGHTSQL_PREDICATE_FLOAT;
		case NUMERICOID:
			return FLIGHTSQL_PREDICATE_NUMERIC;
		case TEXTOID:
		case VARCHAROID:
			return FLIGHTSQL_PREDICATE_STRING;
		case DATEOID:
			return FLIGHTSQL_PREDICATE_DATE;
		case TIMEOID:
			return FLIGHTSQL_PREDICATE_TIME;
		case TIMESTAMPOID:
			return FLIGHTSQL_PREDICATE_TIMESTAMP;
		default:
			return af_type_is_enum(typid) ?
				FLIGHTSQL_PREDICATE_ENUM :
				FLIGHTSQL_PREDICATE_UNSUPPORTED;
	}
}

static bool
flightsql_string_is_safe(const char *value)
{
	const unsigned char *cursor = (const unsigned char *) value;

	for (; *cursor != '\0'; cursor++)
	{
		if (*cursor == '\\' || *cursor < 0x20 || *cursor == 0x7f)
			return false;
	}
	return true;
}

static bool
flightsql_literal_is_safe(Oid typid, Datum value, bool is_null)
{
	FlightSqlPredicateType type = flightsql_predicate_type(typid);

	if (is_null)
		return type != FLIGHTSQL_PREDICATE_UNSUPPORTED;

	switch (type)
	{
		case FLIGHTSQL_PREDICATE_BOOL:
		case FLIGHTSQL_PREDICATE_INTEGER:
			return true;

		case FLIGHTSQL_PREDICATE_FLOAT:
			if (typid == FLOAT4OID)
				return isfinite(DatumGetFloat4(value));
			return isfinite(DatumGetFloat8(value));

		case FLIGHTSQL_PREDICATE_NUMERIC:
			return !numeric_is_nan(DatumGetNumeric(value));

		case FLIGHTSQL_PREDICATE_STRING:
		case FLIGHTSQL_PREDICATE_ENUM:
			{
				Oid			output_function;
				bool		is_varlena;
				char	   *output;
				bool		safe;

				getTypeOutputInfo(typid, &output_function, &is_varlena);
				output = OidOutputFunctionCall(output_function, value);
				safe = flightsql_string_is_safe(output);
				pfree(output);
				return safe;
			}

		case FLIGHTSQL_PREDICATE_DATE:
			{
				DateADT		date = DatumGetDateADT(value);
				int			year;
				int			month;
				int			day;

				if (DATE_NOT_FINITE(date) || !IS_VALID_DATE(date))
					return false;
				j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);
				return year >= 1 && year <= 9999;
			}

		case FLIGHTSQL_PREDICATE_TIME:
			{
				TimeADT		time = DatumGetTimeADT(value);

				return time >= 0 && time < USECS_PER_DAY;
			}

		case FLIGHTSQL_PREDICATE_TIMESTAMP:
			{
				Timestamp	timestamp = DatumGetTimestamp(value);
				struct pg_tm tm;
				fsec_t		fsec;

				if (TIMESTAMP_NOT_FINITE(timestamp) ||
					!IS_VALID_TIMESTAMP(timestamp))
					return false;
				if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) != 0)
					return false;
				return tm.tm_year >= 1 && tm.tm_year <= 9999 && fsec == 0;
			}

		case FLIGHTSQL_PREDICATE_UNSUPPORTED:
			break;
	}
	return false;
}

static bool
flightsql_operator_info(Oid opno, char *name, Size name_size,
						Oid *function_id)
{
	HeapTuple	tuple;
	Form_pg_operator operator_form;
	bool		result = false;

	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", opno);

	operator_form = (Form_pg_operator) GETSTRUCT(tuple);
	if (operator_form->oprnamespace == PG_CATALOG_NAMESPACE &&
		operator_form->oprkind == 'b')
	{
		strlcpy(name, NameStr(operator_form->oprname), name_size);
		*function_id = operator_form->oprcode;
		result = true;
	}
	ReleaseSysCache(tuple);
	return result;
}

static bool
flightsql_operator_is_comparison(const char *name)
{
	return strcmp(name, "=") == 0 ||
		strcmp(name, "<>") == 0 ||
		strcmp(name, "<") == 0 ||
		strcmp(name, "<=") == 0 ||
		strcmp(name, ">") == 0 ||
		strcmp(name, ">=") == 0;
}

static bool
flightsql_operator_is_equality(const char *name)
{
	return strcmp(name, "=") == 0 || strcmp(name, "<>") == 0;
}

static bool
flightsql_comparison_is_safe(Oid left_type, Oid right_type,
							 const char *operator_name,
							 Oid input_collation)
{
	FlightSqlPredicateType left = flightsql_predicate_type(left_type);
	FlightSqlPredicateType right = flightsql_predicate_type(right_type);

	if (!flightsql_operator_is_comparison(operator_name))
		return false;

	if (left == FLIGHTSQL_PREDICATE_ENUM ||
		right == FLIGHTSQL_PREDICATE_ENUM)
		return left == FLIGHTSQL_PREDICATE_ENUM &&
			right == FLIGHTSQL_PREDICATE_ENUM &&
			left_type == right_type &&
			flightsql_operator_is_equality(operator_name);

	if (left == FLIGHTSQL_PREDICATE_INTEGER &&
		right == FLIGHTSQL_PREDICATE_INTEGER)
		return true;

	if ((left == FLIGHTSQL_PREDICATE_INTEGER ||
		 left == FLIGHTSQL_PREDICATE_NUMERIC) &&
		(right == FLIGHTSQL_PREDICATE_INTEGER ||
		 right == FLIGHTSQL_PREDICATE_NUMERIC))
		return true;

	if (left == FLIGHTSQL_PREDICATE_FLOAT &&
		right == FLIGHTSQL_PREDICATE_FLOAT)
		return flightsql_operator_is_equality(operator_name);

	if (left == FLIGHTSQL_PREDICATE_BOOL &&
		right == FLIGHTSQL_PREDICATE_BOOL)
		return flightsql_operator_is_equality(operator_name);

	if (left == FLIGHTSQL_PREDICATE_STRING &&
		right == FLIGHTSQL_PREDICATE_STRING)
	{
		if (!flightsql_operator_is_equality(operator_name))
			return false;
		return !OidIsValid(input_collation) ||
			get_collation_isdeterministic(input_collation);
	}

	return left == right &&
		(left == FLIGHTSQL_PREDICATE_DATE ||
		 left == FLIGHTSQL_PREDICATE_TIMESTAMP);
}

static bool
flightsql_var_is_safe(Node *node, Index scan_relid)
{
	Var		   *var;

	node = flightsql_strip_relabel(node);
	if (node == NULL || !IsA(node, Var))
		return false;

	var = (Var *) node;
	return var->varno == scan_relid &&
		var->varlevelsup == 0 &&
		var->varattno > 0;
}

static bool
flightsql_operand_is_safe(Node *node, Index scan_relid, bool *has_var)
{
	node = flightsql_strip_relabel(node);
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (!flightsql_var_is_safe(node, scan_relid) ||
			flightsql_predicate_type(var->vartype) ==
			FLIGHTSQL_PREDICATE_UNSUPPORTED)
			return false;
		*has_var = true;
		return true;
	}

	if (IsA(node, Const))
	{
		Const	   *constant = (Const *) node;

		return flightsql_literal_is_safe(constant->consttype,
										 constant->constvalue,
										 constant->constisnull);
	}
	return false;
}

static bool
flightsql_array_is_safe(Node *node, Oid scalar_type, Oid input_collation)
{
	node = flightsql_strip_relabel(node);
	if (node == NULL)
		return false;

	if (IsA(node, ArrayExpr))
	{
		ArrayExpr  *array = (ArrayExpr *) node;
		ListCell   *lc;
		int			count = list_length(array->elements);

		if (array->multidims || count == 0 ||
			count > FLIGHTSQL_MAX_IN_VALUES)
			return false;

		foreach(lc, array->elements)
		{
			Node	   *element = flightsql_strip_relabel((Node *) lfirst(lc));
			Const	   *constant;

			if (!IsA(element, Const))
				return false;
			constant = (Const *) element;
			if (constant->constisnull ||
				!flightsql_comparison_is_safe(scalar_type,
											  constant->consttype,
											  "=", input_collation) ||
				!flightsql_literal_is_safe(constant->consttype,
										   constant->constvalue,
										   constant->constisnull))
				return false;
		}
		return true;
	}

	if (IsA(node, Const))
	{
		Const	   *constant = (Const *) node;
		ArrayType  *array;
		Oid			element_type;
		int16		element_length;
		bool		element_by_value;
		char		element_alignment;
		Datum	   *values;
		bool	   *nulls;
		int			count;

		if (constant->constisnull)
			return false;
		array = DatumGetArrayTypeP(constant->constvalue);
		if (ARR_NDIM(array) != 1)
			return false;
		element_type = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(element_type, &element_length,
							&element_by_value, &element_alignment);
		deconstruct_array(array, element_type, element_length,
						  element_by_value, element_alignment,
						  &values, &nulls, &count);
		if (count == 0 || count > FLIGHTSQL_MAX_IN_VALUES ||
			!flightsql_comparison_is_safe(scalar_type, element_type,
										  "=", input_collation))
			return false;
		for (int i = 0; i < count; i++)
		{
			if (nulls[i] ||
				!flightsql_literal_is_safe(element_type, values[i], false))
				return false;
		}
		return true;
	}
	return false;
}

static bool
flightsql_expression_is_safe(Node *node, Index scan_relid,
							 bool require_leakproof, bool *has_var)
{
	node = flightsql_strip_relabel(node);
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				if (var->vartype != BOOLOID)
					return false;
				return flightsql_operand_is_safe(node, scan_relid, has_var);
			}

		case T_Const:
			{
				Const	   *constant = (Const *) node;

				return constant->consttype == BOOLOID &&
					flightsql_literal_is_safe(constant->consttype,
											 constant->constvalue,
											 constant->constisnull);
			}

		case T_OpExpr:
			{
				OpExpr    *expression = (OpExpr *) node;
				Node	   *left;
				Node	   *right;
				char		operator_name[NAMEDATALEN];
				Oid			function_id;
				bool		left_has_var = false;
				bool		right_has_var = false;

				if (list_length(expression->args) != 2 ||
					!flightsql_operator_info(expression->opno,
											 operator_name,
											 sizeof(operator_name),
											 &function_id) ||
					(require_leakproof &&
					 !get_func_leakproof(function_id)))
					return false;
				left = flightsql_strip_relabel(
					(Node *) linitial(expression->args));
				right = flightsql_strip_relabel(
					(Node *) lsecond(expression->args));
				if (!flightsql_operand_is_safe(left, scan_relid,
											   &left_has_var) ||
					!flightsql_operand_is_safe(right, scan_relid,
											   &right_has_var) ||
					left_has_var == right_has_var ||
					!flightsql_comparison_is_safe(exprType(left),
												  exprType(right),
												  operator_name,
												  expression->inputcollid))
					return false;
				*has_var = *has_var || left_has_var || right_has_var;
				return true;
			}

		case T_NullTest:
			{
				NullTest   *test = (NullTest *) node;

				if (test->argisrow ||
					(test->nulltesttype != IS_NULL &&
					 test->nulltesttype != IS_NOT_NULL) ||
					!flightsql_var_is_safe((Node *) test->arg, scan_relid))
					return false;
				*has_var = true;
				return true;
			}

		case T_BooleanTest:
			{
				BooleanTest *test = (BooleanTest *) node;

				if ((test->booltesttype != IS_UNKNOWN &&
					 test->booltesttype != IS_NOT_UNKNOWN) ||
					!flightsql_var_is_safe((Node *) test->arg, scan_relid))
					return false;
				*has_var = true;
				return true;
			}

		case T_BoolExpr:
			{
				BoolExpr   *expression = (BoolExpr *) node;
				ListCell   *lc;

				if (expression->boolop == NOT_EXPR &&
					list_length(expression->args) != 1)
					return false;
				if ((expression->boolop == AND_EXPR ||
					 expression->boolop == OR_EXPR) &&
					expression->args == NIL)
					return false;

				foreach(lc, expression->args)
				if (!flightsql_expression_is_safe(
						(Node *) lfirst(lc), scan_relid,
						require_leakproof, has_var))
					return false;
				return true;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expression = (ScalarArrayOpExpr *) node;
				Node	   *left;
				Node	   *right;
				char		operator_name[NAMEDATALEN];
				Oid			function_id;
				bool		left_has_var = false;

				if (list_length(expression->args) != 2 ||
					!flightsql_operator_info(expression->opno,
											 operator_name,
											 sizeof(operator_name),
											 &function_id) ||
					(require_leakproof &&
					 !get_func_leakproof(function_id)) ||
					!((expression->useOr &&
					   strcmp(operator_name, "=") == 0) ||
					  (!expression->useOr &&
					   strcmp(operator_name, "<>") == 0)))
					return false;

				left = flightsql_strip_relabel(
					(Node *) linitial(expression->args));
				right = flightsql_strip_relabel(
					(Node *) lsecond(expression->args));
				if (!IsA(left, Var) ||
					!flightsql_operand_is_safe(left, scan_relid,
											   &left_has_var) ||
					!left_has_var ||
					!flightsql_array_is_safe(right, exprType(left),
											 expression->inputcollid))
					return false;
				*has_var = true;
				return true;
			}

		default:
			break;
	}
	return false;
}

static char *
flightsql_column_name(Oid foreigntableid, AttrNumber attnum)
{
	List	   *options = GetForeignColumnOptions(foreigntableid, attnum);
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *option = (DefElem *) lfirst(lc);

		if (strcmp(option->defname, "column_name") == 0)
			return defGetString(option);
	}
	return get_attname(foreigntableid, attnum, false);
}

static void
flightsql_append_quoted_string(StringInfo buf, const char *value)
{
	appendStringInfoChar(buf, '\'');
	for (const char *cursor = value; *cursor != '\0'; cursor++)
	{
		if (*cursor == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *cursor);
	}
	appendStringInfoChar(buf, '\'');
}

static void
flightsql_append_date(StringInfo buf, DateADT value)
{
	int			year;
	int			month;
	int			day;

	j2date(value + POSTGRES_EPOCH_JDATE, &year, &month, &day);
	appendStringInfo(buf, "DATE '%04d-%02d-%02d'", year, month, day);
}

static void
flightsql_append_time(StringInfo buf, TimeADT value)
{
	int64		hours;
	int64		minutes;
	int64		seconds;
	int64		microseconds;

	hours = value / USECS_PER_HOUR;
	value %= USECS_PER_HOUR;
	minutes = value / USECS_PER_MINUTE;
	value %= USECS_PER_MINUTE;
	seconds = value / USECS_PER_SEC;
	microseconds = value % USECS_PER_SEC;

	appendStringInfo(buf, "TIME '%02lld:%02lld:%02lld.%06lld'",
					 (long long) hours, (long long) minutes,
					 (long long) seconds, (long long) microseconds);
}

static void
flightsql_append_timestamp(StringInfo buf, Timestamp value)
{
	struct pg_tm tm;
	fsec_t		fsec;

	if (timestamp2tm(value, NULL, &tm, &fsec, NULL, NULL) != 0)
		elog(ERROR, "could not deparse Flight SQL timestamp");
	if (fsec != 0)
		elog(ERROR, "could not deparse fractional Flight SQL timestamp");
	appendStringInfo(buf,
					 "TIMESTAMP '%04d-%02d-%02d %02d:%02d:%02d'",
					 tm.tm_year, tm.tm_mon, tm.tm_mday,
					 tm.tm_hour, tm.tm_min, tm.tm_sec);
}

static void
flightsql_append_literal(StringInfo buf, Oid typid, Datum value, bool is_null)
{
	Oid			output_function;
	bool		is_varlena;
	char	   *output;

	if (is_null)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	switch (flightsql_predicate_type(typid))
	{
		case FLIGHTSQL_PREDICATE_BOOL:
			appendStringInfoString(buf,
								   DatumGetBool(value) ? "TRUE" : "FALSE");
			return;

		case FLIGHTSQL_PREDICATE_DATE:
			flightsql_append_date(buf, DatumGetDateADT(value));
			return;

		case FLIGHTSQL_PREDICATE_TIME:
			flightsql_append_time(buf, DatumGetTimeADT(value));
			return;

		case FLIGHTSQL_PREDICATE_TIMESTAMP:
			flightsql_append_timestamp(buf, DatumGetTimestamp(value));
			return;

		case FLIGHTSQL_PREDICATE_STRING:
		case FLIGHTSQL_PREDICATE_ENUM:
			getTypeOutputInfo(typid, &output_function, &is_varlena);
			output = OidOutputFunctionCall(output_function, value);
			flightsql_append_quoted_string(buf, output);
			pfree(output);
			return;

		case FLIGHTSQL_PREDICATE_INTEGER:
		case FLIGHTSQL_PREDICATE_FLOAT:
		case FLIGHTSQL_PREDICATE_NUMERIC:
			getTypeOutputInfo(typid, &output_function, &is_varlena);
			output = OidOutputFunctionCall(output_function, value);
			appendStringInfoString(buf, output);
			pfree(output);
			return;

		case FLIGHTSQL_PREDICATE_UNSUPPORTED:
			break;
	}
	elog(ERROR, "unsupported Flight SQL predicate literal type %u", typid);
}

static void
flightsql_deparse_expression(Node *node, FlightSqlDeparseContext *context)
{
	node = flightsql_strip_relabel(node);

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;
				char	   *column_name =
					flightsql_column_name(context->foreigntableid,
										  var->varattno);

				appendStringInfoString(context->buf,
									   quote_identifier(column_name));
				return;
			}

		case T_Const:
			{
				Const	   *constant = (Const *) node;

				flightsql_append_literal(context->buf,
										 constant->consttype,
										 constant->constvalue,
										 constant->constisnull);
				return;
			}

		case T_OpExpr:
			{
				OpExpr    *expression = (OpExpr *) node;
				char	   *operator_name = get_opname(expression->opno);

				appendStringInfoChar(context->buf, '(');
				flightsql_deparse_expression(
					(Node *) linitial(expression->args), context);
				appendStringInfo(context->buf, " %s ", operator_name);
				flightsql_deparse_expression(
					(Node *) lsecond(expression->args), context);
				appendStringInfoChar(context->buf, ')');
				pfree(operator_name);
				return;
			}

		case T_NullTest:
			{
				NullTest   *test = (NullTest *) node;

				appendStringInfoChar(context->buf, '(');
				flightsql_deparse_expression((Node *) test->arg, context);
				appendStringInfoString(context->buf,
									   test->nulltesttype == IS_NULL ?
									   " IS NULL)" : " IS NOT NULL)");
				return;
			}

		case T_BooleanTest:
			{
				BooleanTest *test = (BooleanTest *) node;

				appendStringInfoChar(context->buf, '(');
				flightsql_deparse_expression((Node *) test->arg, context);
				appendStringInfoString(context->buf,
									   test->booltesttype == IS_UNKNOWN ?
									   " IS NULL)" : " IS NOT NULL)");
				return;
			}

		case T_BoolExpr:
			{
				BoolExpr   *expression = (BoolExpr *) node;
				ListCell   *lc;
				const char *separator =
					expression->boolop == AND_EXPR ? " AND " : " OR ";
				bool		first = true;

				appendStringInfoChar(context->buf, '(');
				if (expression->boolop == NOT_EXPR)
					appendStringInfoString(context->buf, "NOT ");
				foreach(lc, expression->args)
				{
					if (!first)
						appendStringInfoString(context->buf, separator);
					flightsql_deparse_expression((Node *) lfirst(lc),
												 context);
					first = false;
				}
				appendStringInfoChar(context->buf, ')');
				return;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expression = (ScalarArrayOpExpr *) node;

				appendStringInfoChar(context->buf, '(');
				flightsql_deparse_expression(
					(Node *) linitial(expression->args), context);
				appendStringInfoString(context->buf,
									   expression->useOr ?
									   " IN (" : " NOT IN (");
				flightsql_deparse_array(
					(Node *) lsecond(expression->args), context);
				appendStringInfoString(context->buf, "))");
				return;
			}

		default:
			break;
	}
	elog(ERROR, "unsupported node type in Flight SQL predicate: %d",
		 (int) nodeTag(node));
}

static void
flightsql_deparse_array(Node *node, FlightSqlDeparseContext *context)
{
	bool		first = true;

	node = flightsql_strip_relabel(node);
	if (IsA(node, ArrayExpr))
	{
		ArrayExpr  *array = (ArrayExpr *) node;
		ListCell   *lc;

		foreach(lc, array->elements)
		{
			if (!first)
				appendStringInfoString(context->buf, ", ");
			flightsql_deparse_expression((Node *) lfirst(lc), context);
			first = false;
		}
		return;
	}

	if (IsA(node, Const))
	{
		Const	   *constant = (Const *) node;
		ArrayType  *array = DatumGetArrayTypeP(constant->constvalue);
		Oid			element_type = ARR_ELEMTYPE(array);
		int16		element_length;
		bool		element_by_value;
		char		element_alignment;
		Datum	   *values;
		bool	   *nulls;
		int			count;

		get_typlenbyvalalign(element_type, &element_length,
							&element_by_value, &element_alignment);
		deconstruct_array(array, element_type, element_length,
						  element_by_value, element_alignment,
						  &values, &nulls, &count);
		for (int i = 0; i < count; i++)
		{
			if (!first)
				appendStringInfoString(context->buf, ", ");
			flightsql_append_literal(context->buf, element_type,
									 values[i], nulls[i]);
			first = false;
		}
		return;
	}
	elog(ERROR, "unsupported Flight SQL IN-list representation");
}
