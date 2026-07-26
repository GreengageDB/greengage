/*
 *	pg_upgrade_support.c
 *
 *	server-side functions to set backend global variables
 *	to control oid and relfilenode assignment, and do other special
 *	hacks needed for pg_upgrade.
 *
 *	Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *	src/backend/utils/adt/pg_upgrade_support.c
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/transam.h"
#include "catalog/binary_upgrade.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

#define CHECK_IS_BINARY_UPGRADE									\
do {															\
	if (!IsBinaryUpgrade)										\
		ereport(ERROR,											\
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),	\
				 (errmsg("function can only be called when server is in binary upgrade mode")))); \
} while (0)

typedef struct {
	List *names;
	Oid  schema;
} CreatedNamesPerSchema;

static List *createdArrayNames = NIL;

/*
 * isNamePreassigned
 *     Outputs whether type name arg was already assigned during upgrade.
 */
static bool
isNamePreassigned(const char *arr, Oid typeNamespace)
{
	CreatedNamesPerSchema *target = NULL;
	ListCell *lc;
	char *name;
	foreach(lc, createdArrayNames)
	{
		CreatedNamesPerSchema *names = lfirst(lc);
		if (names->schema == typeNamespace)
		{
			target = names;
			break;
		}
	}
	if (target)
	{
		foreach (lc, target->names)
		{
			name = lfirst(lc);
			if (strcmp(arr, name) == 0)
				return true;
		}
	}
	return false;
}

static void
RememberCreatedName(char *typname, Oid schema)
{
	CreatedNamesPerSchema *target = NULL;
	ListCell *lc;
	char* persistentTypeName = pstrdup(typname);

	foreach(lc, createdArrayNames)
	{
		CreatedNamesPerSchema *names = lfirst(lc);
		if (names->schema == schema)
		{
			target = names;
			break;
		}
	}
	if (!target)
	{
		target = palloc0(sizeof(CreatedNamesPerSchema));
		target->schema = schema;
		target->names = NIL;
		createdArrayNames = lappend(createdArrayNames, target);
	}

	target->names = lappend(target->names, persistentTypeName);
}

/*
 * makeArrayTypeNameUpgrade
 *     A wrapper around makeArrayTypeName, that also checks if the newly
 *     generated name collides with the ones already assigned
 *     during upgrade.
 */
static char *
makeArrayTypeNameUpgrade(const char *typeName, Oid typeNamespace)
{
	char *arr, *modifiedTypeName;
	int typeNameLen = strlen(typeName);
	int underscores = 0;

	modifiedTypeName = palloc(NAMEDATALEN);
	arr = makeArrayTypeName(typeName, typeNamespace);
	while (isNamePreassigned(arr, typeNamespace))
	{
		underscores++;
		if (typeNameLen + underscores >= NAMEDATALEN)
			ereport(ERROR,
				   (errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("could not form array type name for type \"%s\"",
							typeName)));
		memset(modifiedTypeName, '_', underscores);
		strcpy(modifiedTypeName + underscores, typeName);
		arr = makeArrayTypeName(modifiedTypeName, typeNamespace);
	}
	pfree(modifiedTypeName);

	return arr;
}

Datum
binary_upgrade_set_next_pg_type_oid(PG_FUNCTION_ARGS)
{
	MemoryContext oldctx;
	Oid			typoid = PG_GETARG_OID(0);
	Oid			typnamespaceoid = PG_GETARG_OID(1);
	char	   *typname = GET_STR(PG_GETARG_TEXT_P(2));

	CHECK_IS_BINARY_UPGRADE;

	/* Remember that we've already taken this name */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	RememberCreatedName(typname, typnamespaceoid);
	MemoryContextSwitchTo(oldctx);

	AddPreassignedOidFromBinaryUpgrade(typoid, TypeRelationId, typname,
						typnamespaceoid, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_array_pg_type_oid(PG_FUNCTION_ARGS)
{
	MemoryContext oldctx;
	Oid			typoid = PG_GETARG_OID(0);
	Oid         old_type_oid;
	bool        exists;
	Oid			typnamespaceoid = PG_GETARG_OID(1);
	char	   *relname = GET_STR(PG_GETARG_TEXT_P(2));
	char       *typname;
	ListCell   *lc;

	CHECK_IS_BINARY_UPGRADE;

	old_type_oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
								   CStringGetDatum(relname),
								   ObjectIdGetDatum(typnamespaceoid));

	exists = OidIsValid(old_type_oid);

	foreach(lc, createdArrayNames)
	{
		char *name = (char *) lfirst(lc);
		if (strcmp(relname, name) == 0)
		{
			exists = true;
			break;
		}
	}

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	typname = makeArrayTypeNameUpgrade(relname, typnamespaceoid);
	RememberCreatedName(typname, typnamespaceoid);
	if (exists)
	{
		/*
		 * When database wants to create a regular type, and
		 * there is already an array type with the same name
		 * in the cluster, exising array type would be renamed,
		 * getting the next free name via an additional call to
		 * makeArrayType. Meaning that the next array type name
		 * needs two makeArrayType calls to get to.
		 *
		 * Note, that we assume that the base type for this array type
		 * is always created.
		 *
		 * Also, the whole logic descripted here works only if existing
		 * type is an array type. If it not, it wouldn't be possible
		 * to move existing type, and upgrade would fail. But it would
		 * happed regardless of what we are doing here, so let's
		 * not complicate the logic.
		 */
		typname = makeArrayTypeNameUpgrade(typname, typnamespaceoid);
		RememberCreatedName(typname, typnamespaceoid);
	}
	MemoryContextSwitchTo(oldctx);

	AddPreassignedOidFromBinaryUpgrade(typoid, TypeRelationId, typname,
						typnamespaceoid, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Oid			relnamespace = PG_GETARG_OID(1);
	char	   *relname = GET_STR(PG_GETARG_TEXT_P(2));

	CHECK_IS_BINARY_UPGRADE;
	AddPreassignedOidFromBinaryUpgrade(reloid, RelationRelationId, relname,
									   relnamespace, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Oid			relnamespace = PG_GETARG_OID(1);
	char	   *relname = GET_STR(PG_GETARG_TEXT_P(2));

	CHECK_IS_BINARY_UPGRADE;
	AddPreassignedOidFromBinaryUpgrade(reloid, RelationRelationId, relname,
									   relnamespace, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_toast_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Oid			relnamespace = PG_GETARG_OID(1);
	char		*relname = GET_STR(PG_GETARG_TEXT_P(2));

	CHECK_IS_BINARY_UPGRADE;
	AddPreassignedOidFromBinaryUpgrade(reloid, RelationRelationId, relname,
									   relnamespace, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_pg_enum_oid(PG_FUNCTION_ARGS)
{
	Oid			enumoid = PG_GETARG_OID(0);
	Oid			typeoid = PG_GETARG_OID(1);
	char	   *enumlabel = GET_STR(PG_GETARG_TEXT_P(2));

	CHECK_IS_BINARY_UPGRADE;
	AddPreassignedOidFromBinaryUpgrade(enumoid, EnumRelationId, enumlabel,
									   InvalidOid, typeoid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_pg_authid_oid(PG_FUNCTION_ARGS)
{
	Oid			authoid = PG_GETARG_OID(0);
	char	   *rolename = GET_STR(PG_GETARG_TEXT_P(1));

	CHECK_IS_BINARY_UPGRADE;
	if (Gp_role == GP_ROLE_UTILITY)
	{
		AddPreassignedOidFromBinaryUpgrade(authoid, AuthIdRelationId, rolename,
										   InvalidOid, InvalidOid, InvalidOid);
	}

	PG_RETURN_VOID();
}

Datum
binary_upgrade_create_empty_extension(PG_FUNCTION_ARGS)
{
	text	   *extName;
	text	   *schemaName;
	bool		relocatable;
	text	   *extVersion;
	Datum		extConfig;
	Datum		extCondition;
	List	   *requiredExtensions;

	CHECK_IS_BINARY_UPGRADE;

	/* We must check these things before dereferencing the arguments */
	if (PG_ARGISNULL(0) ||
		PG_ARGISNULL(1) ||
		PG_ARGISNULL(2) ||
		PG_ARGISNULL(3))
		elog(ERROR, "null argument to binary_upgrade_create_empty_extension is not allowed");

	extName = PG_GETARG_TEXT_PP(0);
	schemaName = PG_GETARG_TEXT_PP(1);
	relocatable = PG_GETARG_BOOL(2);
	extVersion = PG_GETARG_TEXT_PP(3);

	if (PG_ARGISNULL(4))
		extConfig = PointerGetDatum(NULL);
	else
		extConfig = PG_GETARG_DATUM(4);

	if (PG_ARGISNULL(5))
		extCondition = PointerGetDatum(NULL);
	else
		extCondition = PG_GETARG_DATUM(5);

	requiredExtensions = NIL;
	if (!PG_ARGISNULL(6))
	{
		ArrayType  *textArray = PG_GETARG_ARRAYTYPE_P(6);
		Datum	   *textDatums;
		int			ndatums;
		int			i;

		deconstruct_array(textArray,
						  TEXTOID, -1, false, 'i',
						  &textDatums, NULL, &ndatums);
		for (i = 0; i < ndatums; i++)
		{
			char	   *extName = TextDatumGetCString(textDatums[i]);
			Oid			extOid = get_extension_oid(extName, false);

			requiredExtensions = lappend_oid(requiredExtensions, extOid);
		}
	}

	InsertExtensionTuple(text_to_cstring(extName),
						 GetUserId(),
						 get_namespace_oid(text_to_cstring(schemaName), false),
						 relocatable,
						 text_to_cstring(extVersion),
						 extConfig,
						 extCondition,
						 requiredExtensions);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_record_init_privs(PG_FUNCTION_ARGS)
{
	bool		record_init_privs = PG_GETARG_BOOL(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_record_init_privs = record_init_privs;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_pg_namespace_oid(PG_FUNCTION_ARGS)
{
	Oid			nspid = PG_GETARG_OID(0);
	char	   *nspname = GET_STR(PG_GETARG_TEXT_P(1));

	CHECK_IS_BINARY_UPGRADE;

	if (Gp_role == GP_ROLE_UTILITY)
	{
		AddPreassignedOidFromBinaryUpgrade(nspid, NamespaceRelationId, nspname,
										   InvalidOid, InvalidOid, InvalidOid);
	}

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_preassigned_oids(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *oids;
	int			nelems;
	int			i;

	CHECK_IS_BINARY_UPGRADE;

	deconstruct_array(array, OIDOID, sizeof(Oid), true, 'i',
					  &oids, NULL, &nelems);

	for (i = 0; i < nelems; i++)
	{
		Datum		oid = DatumGetObjectId(oids[i]);

		MarkOidPreassignedFromBinaryUpgrade(oid);
	}

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_missing_value(PG_FUNCTION_ARGS)
{
	Oid			table_id = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	text	   *value = PG_GETARG_TEXT_P(2);
	char	   *cattname = text_to_cstring(attname);
	char	   *cvalue = text_to_cstring(value);

	CHECK_IS_BINARY_UPGRADE;
	SetAttrMissing(table_id, cattname, cvalue);

	PG_RETURN_VOID();
}
