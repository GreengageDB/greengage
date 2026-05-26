/*
 *	common.h
 *		Common support routines for bin/scripts/
 *
 *	Copyright (c) 2003-2021, PostgreSQL Global Development Group
 *
 *	src/bin/scripts/common.h
 */
#ifndef COMMON_H
#define COMMON_H

#include "common/username.h"
#include "getopt_long.h"		/* pgrminclude ignore */
#include "libpq-fe.h"
#include "pqexpbuffer.h"		/* pgrminclude ignore */

typedef void (*help_handler) (const char *progname);

enum trivalue
{
	TRI_DEFAULT,
	TRI_NO,
	TRI_YES
};

extern PGconn *connectDatabase(const char *dbname, const char *pghost,
							   const char *pgport, const char *pguser,
							   enum trivalue prompt_password,
							   const char *progname, bool echo,
							   bool fail_ok, bool allow_password_reuse);
extern PGconn *connectMaintenanceDatabase(const char *maintenance_db,
										  const char *pghost, const char *pgport,
										  const char *pguser, enum trivalue prompt_password,
										  const char *progname, bool echo);
extern void disconnectDatabase(PGconn *conn);

extern void splitTableColumnsSpec(const char *spec, int encoding,
								  char **table, const char **columns);

extern void appendQualifiedRelation(PQExpBuffer buf, const char *name,
									PGconn *conn, bool echo);

extern bool yesno_prompt(const char *question);

extern bool executeMaintenanceCommand(PGconn *conn, const char *query, bool echo);

extern bool processQueryResult(PGconn *conn, PGresult *result);

#endif							/* COMMON_H */
