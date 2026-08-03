/*-------------------------------------------------------------------------
 *
 * binary_upgrade.h
 *	  variables used for binary upgrades
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/binary_upgrade.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BINARY_UPGRADE_H
#define BINARY_UPGRADE_H

#include "common/relpath.h"

/*
 * These variables are honored during binary upgrade, matching upstream.
 * (GPDB additionally uses its generic OID pre-assignment machinery for
 * ordinary OID assignment, but the relfilenumber overrides and the
 * pg_type/enum/authid and record_init_privs variables below are used here.)
 */
extern PGDLLIMPORT Oid binary_upgrade_next_pg_tablespace_oid;

extern PGDLLIMPORT Oid binary_upgrade_next_pg_type_oid;
extern PGDLLIMPORT Oid binary_upgrade_next_array_pg_type_oid;
extern PGDLLIMPORT Oid binary_upgrade_next_mrng_pg_type_oid;
extern PGDLLIMPORT Oid binary_upgrade_next_mrng_array_pg_type_oid;

extern PGDLLIMPORT Oid binary_upgrade_next_heap_pg_class_oid;
extern PGDLLIMPORT RelFileNumber binary_upgrade_next_heap_pg_class_relfilenumber;
extern PGDLLIMPORT Oid binary_upgrade_next_index_pg_class_oid;
extern PGDLLIMPORT RelFileNumber binary_upgrade_next_index_pg_class_relfilenumber;
extern PGDLLIMPORT Oid binary_upgrade_next_toast_pg_class_oid;
extern PGDLLIMPORT RelFileNumber binary_upgrade_next_toast_pg_class_relfilenumber;

extern PGDLLIMPORT Oid binary_upgrade_next_pg_enum_oid;
extern PGDLLIMPORT Oid binary_upgrade_next_pg_authid_oid;

extern PGDLLIMPORT bool binary_upgrade_record_init_privs;

#endif							/* BINARY_UPGRADE_H */
