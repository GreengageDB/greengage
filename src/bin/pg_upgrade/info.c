/*
 *	info.c
 *
 *	information support functions
 *
 *	Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/info.c
 */

#include "postgres_fe.h"

#include "access/transam.h"
#include "catalog/pg_class_d.h"
#include "greengage/pg_upgrade_greengage.h"
#include "pg_upgrade.h"
#include "pqexpbuffer.h"

static void create_rel_filename_map(const char *old_data, const char *new_data,
									const DbInfo *old_db, const DbInfo *new_db,
									const RelInfo *old_rel, const RelInfo *new_rel,
									FileNameMap *map);
static void report_unmatched_relation(const RelInfo *rel, const DbInfo *db,
									  bool is_new_db);
static void free_db_and_rel_infos(DbInfoArr *db_arr);
static void get_template0_info(ClusterInfo *cluster);
static void get_db_infos(ClusterInfo *cluster);
static char *get_rel_infos_query(ClusterInfo *cluster);
static void process_rel_infos(DbInfo *dbinfo, PGresult *res, void *arg);
static void free_rel_infos(RelInfoArr *rel_arr);
static void print_db_infos(DbInfoArr *db_arr);
static void print_rel_infos(RelInfoArr *rel_arr);
static void print_slot_infos(LogicalSlotInfoArr *slot_arr);
static char *get_old_cluster_logical_slot_infos_query(void);
static void process_old_cluster_logical_slot_infos(DbInfo *dbinfo, PGresult *res, void *arg);


/*
 * gen_db_file_maps()
 *
 * generates a database mapping from "old_db" to "new_db".
 *
 * Returns a malloc'ed array of mappings.  The length of the array
 * is returned into *nmaps.
 */
FileNameMap *
gen_db_file_maps(DbInfo *old_db, DbInfo *new_db,
				 int *nmaps,
				 const char *old_pgdata, const char *new_pgdata)
{
	FileNameMap *maps;
	int			old_relnum,
				new_relnum;
	int			num_maps = 0;
	bool		all_matched = true;

	/* There will certainly not be more mappings than there are old rels */
	maps = (FileNameMap *) pg_malloc(sizeof(FileNameMap) *
									 old_db->rel_arr.nrels);

	/*
	 * Each of the RelInfo arrays should be sorted by OID.  Scan through them
	 * and match them up.  If we fail to match everything, we'll abort, but
	 * first print as much info as we can about mismatches.
	 */
	old_relnum = new_relnum = 0;
	while (old_relnum < old_db->rel_arr.nrels ||
		   new_relnum < new_db->rel_arr.nrels)
	{
		RelInfo    *old_rel = (old_relnum < old_db->rel_arr.nrels) ?
			&old_db->rel_arr.rels[old_relnum] : NULL;
		RelInfo    *new_rel = (new_relnum < new_db->rel_arr.nrels) ?
			&new_db->rel_arr.rels[new_relnum] : NULL;

		/* handle running off one array before the other */
		if (!new_rel)
		{
			/*
			 * old_rel is unmatched.  This should never happen, because we
			 * force new rels to have TOAST tables if the old one did.
			 */
			report_unmatched_relation(old_rel, old_db, false);
			all_matched = false;
			old_relnum++;
			continue;
		}
		if (!old_rel)
		{
			/*
			 * new_rel is unmatched.  This shouldn't really happen either, but
			 * if it's a TOAST table, we can ignore it and continue
			 * processing, assuming that the new server made a TOAST table
			 * that wasn't needed.
			 */
			if (strcmp(new_rel->nspname, "pg_toast") != 0)
			{
				report_unmatched_relation(new_rel, new_db, true);
				all_matched = false;
			}
			new_relnum++;
			continue;
		}

		/* check for mismatched OID */
		if (old_rel->reloid < new_rel->reloid)
		{
			/* old_rel is unmatched, see comment above */
			report_unmatched_relation(old_rel, old_db, false);
			all_matched = false;
			old_relnum++;
			continue;
		}
		else if (old_rel->reloid > new_rel->reloid)
		{
			/* new_rel is unmatched, see comment above */
			if (strcmp(new_rel->nspname, "pg_toast") != 0)
			{
				report_unmatched_relation(new_rel, new_db, true);
				all_matched = false;
			}
			new_relnum++;
			continue;
		}

		/*
		 * Verify that rels of same OID have same name.  The namespace name
		 * should always match, but the relname might not match for TOAST
		 * tables (and, therefore, their indexes).
		 *
		 * TOAST table names initially match the heap pg_class oid, but
		 * pre-9.0 they can change during certain commands such as CLUSTER, so
		 * don't insist on a match if old cluster is < 9.0.
		 *
		 * XXX GPDB: for TOAST tables, don't insist on a match at all
		 * yet; there are other ways for us to get mismatched names. Ideally
		 * this will go away eventually.
		 */
		if (strcmp(old_rel->nspname, new_rel->nspname) != 0 ||
			(strcmp(old_rel->relname, new_rel->relname) != 0 &&
			 (/* GET_MAJOR_VERSION(old_cluster.major_version) >= 900 || */
			  strcmp(old_rel->nspname, "pg_toast") != 0)))
		{
			pg_log(PG_WARNING, "Relation names for OID %u in database \"%s\" do not match: "
				   "old name \"%s.%s\", new name \"%s.%s\"",
				   old_rel->reloid, old_db->db_name,
				   old_rel->nspname, old_rel->relname,
				   new_rel->nspname, new_rel->relname);
			all_matched = false;
			old_relnum++;
			new_relnum++;
			continue;
		}

		/*
		 * External tables have relfilenodes but no physical files, and aoseg
		 * tables are handled by their AO table
		 */
		if (old_rel->relstorage == 'x' || strcmp(new_rel->nspname, "pg_aoseg") == 0)
		{
			old_relnum++;
			new_relnum++;
			continue;
		}

		/* XXX Why are we doing this here and not in get_rel_infos()? */
		if (old_rel->aosegments != NULL)
			old_rel->reltype = AO;
		else if (old_rel->aocssegments != NULL)
			old_rel->reltype = AOCS;
		else
			old_rel->reltype = HEAP;

		/* OK, create a mapping entry */
		create_rel_filename_map(old_pgdata, new_pgdata, old_db, new_db,
								old_rel, new_rel, maps + num_maps);
		num_maps++;
		old_relnum++;
		new_relnum++;
	}

	if (!all_matched)
		pg_fatal("Failed to match up old and new tables in database \"%s\"",
				 old_db->db_name);

	*nmaps = num_maps;
	return maps;
}


/*
 * create_rel_filename_map()
 *
 * fills a file node map structure and returns it in "map".
 */
static void
create_rel_filename_map(const char *old_data, const char *new_data,
						const DbInfo *old_db, const DbInfo *new_db,
						const RelInfo *old_rel, const RelInfo *new_rel,
						FileNameMap *map)
{
	/* In case old/new tablespaces don't match, do them separately. */
	if (strlen(old_rel->tablespace) == 0)
	{
		/*
		 * relation belongs to the default tablespace, hence relfiles should
		 * exist in the data directories.
		 */
		map->old_tablespace = old_data;
		map->old_tablespace_suffix = "/base";
	}
	else
	{
		/* relation belongs to a tablespace, so use the tablespace location */
		map->old_tablespace = old_rel->tablespace;
		map->old_tablespace_suffix = old_cluster.tablespace_suffix;
	}

	/* Do the same for new tablespaces */
	if (strlen(new_rel->tablespace) == 0)
	{
		map->new_tablespace = new_data;
		map->new_tablespace_suffix = "/base";
	}
	else
	{
		map->new_tablespace = new_rel->tablespace;
		map->new_tablespace_suffix = new_cluster.tablespace_suffix;
	}

	/* DB oid and relfilenumbers are preserved between old and new cluster */
	map->db_oid = old_db->db_oid;
	map->relfilenumber = old_rel->relfilenumber;

	/* GPDB additions to map data */
	map->has_numerics = old_rel->has_numerics;
	map->atts = old_rel->atts;
	map->natts = old_rel->natts;
	map->type = old_rel->reltype;

	/* An AO table doesn't necessarily have segment 0 at all. */
	map->missing_seg0_ok = is_appendonly(old_rel->relstorage);

	/* used only for logging and error reporting, old/new are identical */
	map->nspname = old_rel->nspname;
	map->relname = old_rel->relname;
}


/*
 * Complain about a relation we couldn't match to the other database,
 * identifying it as best we can.
 */
static void
report_unmatched_relation(const RelInfo *rel, const DbInfo *db, bool is_new_db)
{
	Oid			reloid = rel->reloid;	/* we might change rel below */
	char		reldesc[1000];
	int			i;

	snprintf(reldesc, sizeof(reldesc), "\"%s.%s\"",
			 rel->nspname, rel->relname);
	if (rel->indtable)
	{
		for (i = 0; i < db->rel_arr.nrels; i++)
		{
			const RelInfo *hrel = &db->rel_arr.rels[i];

			if (hrel->reloid == rel->indtable)
			{
				snprintf(reldesc + strlen(reldesc),
						 sizeof(reldesc) - strlen(reldesc),
						 _(" which is an index on \"%s.%s\""),
						 hrel->nspname, hrel->relname);
				/* Shift attention to index's table for toast check */
				rel = hrel;
				break;
			}
		}
		if (i >= db->rel_arr.nrels)
			snprintf(reldesc + strlen(reldesc),
					 sizeof(reldesc) - strlen(reldesc),
					 _(" which is an index on OID %u"), rel->indtable);
	}
	if (rel->toastheap)
	{
		for (i = 0; i < db->rel_arr.nrels; i++)
		{
			const RelInfo *brel = &db->rel_arr.rels[i];

			if (brel->reloid == rel->toastheap)
			{
				snprintf(reldesc + strlen(reldesc),
						 sizeof(reldesc) - strlen(reldesc),
						 _(" which is the TOAST table for \"%s.%s\""),
						 brel->nspname, brel->relname);
				break;
			}
		}
		if (i >= db->rel_arr.nrels)
			snprintf(reldesc + strlen(reldesc),
					 sizeof(reldesc) - strlen(reldesc),
					 _(" which is the TOAST table for OID %u"), rel->toastheap);
	}

	if (is_new_db)
		pg_log(PG_WARNING, "No match found in old cluster for new relation with OID %u in database \"%s\": %s",
			   reloid, db->db_name, reldesc);
	else
		pg_log(PG_WARNING, "No match found in new cluster for old relation with OID %u in database \"%s\": %s",
			   reloid, db->db_name, reldesc);
}

/*
 * get_db_rel_and_slot_infos()
 *
 * higher level routine to generate dbinfos for the database running
 * on the given "port". Assumes that server is already running.
 */
void
get_db_rel_and_slot_infos(ClusterInfo *cluster)
{
	UpgradeTask *task = upgrade_task_create();
	char	   *rel_infos_query = NULL;
	char	   *logical_slot_infos_query = NULL;

	if (cluster->dbarr.dbs != NULL)
		free_db_and_rel_infos(&cluster->dbarr);

	get_template0_info(cluster);
	get_db_infos(cluster);

	rel_infos_query = get_rel_infos_query(cluster);
	upgrade_task_add_step(task,
						  rel_infos_query,
						  process_rel_infos,
						  true, cluster);

	/*
	 * Logical slots are only carried over to the new cluster when the old
	 * cluster is on PG17 or newer.  This is because before that the logical
	 * slots are not saved at shutdown, so there is no guarantee that the
	 * latest confirmed_flush_lsn is saved to disk which can lead to data
	 * loss. It is still not guaranteed for manually created slots in PG17, so
	 * subsequent checks done in check_old_cluster_for_valid_slots() would
	 * raise a FATAL error if such slots are included.
	 */
	if (cluster == &old_cluster &&
		GET_MAJOR_VERSION(cluster->major_version) > 1600)
	{
		logical_slot_infos_query = get_old_cluster_logical_slot_infos_query();
		upgrade_task_add_step(task,
							  logical_slot_infos_query,
							  process_old_cluster_logical_slot_infos,
							  true, NULL);
	}

	upgrade_task_run(task, cluster);
	upgrade_task_free(task);

	pg_free(rel_infos_query);
	if (logical_slot_infos_query)
		pg_free(logical_slot_infos_query);

	if (cluster == &old_cluster)
		pg_log(PG_VERBOSE, "\nsource databases:");
	else
		pg_log(PG_VERBOSE, "\ntarget databases:");

	if (log_opts.verbose)
		print_db_infos(&cluster->dbarr);
}


/*
 * Get information about template0, which will be copied from the old cluster
 * to the new cluster.
 */
static void
get_template0_info(ClusterInfo *cluster)
{
	PGconn	   *conn = connectToServer(cluster, "template1");
	DbLocaleInfo *locale;
	PGresult   *dbres;
	int			i_datencoding;
	int			i_datlocprovider;
	int			i_datcollate;
	int			i_datctype;
	int			i_datlocale;

	if (GET_MAJOR_VERSION(cluster->major_version) >= 1700)
		dbres = executeQueryOrDie(conn,
								  "SELECT encoding, datlocprovider, "
								  "       datcollate, datctype, datlocale "
								  "FROM	pg_catalog.pg_database "
								  "WHERE datname='template0'");
	else if (GET_MAJOR_VERSION(cluster->major_version) >= 1500)
		dbres = executeQueryOrDie(conn,
								  "SELECT encoding, datlocprovider, "
								  "       datcollate, datctype, daticulocale AS datlocale "
								  "FROM	pg_catalog.pg_database "
								  "WHERE datname='template0'");
	else
		dbres = executeQueryOrDie(conn,
								  "SELECT encoding, 'c' AS datlocprovider, "
								  "       datcollate, datctype, NULL AS datlocale "
								  "FROM	pg_catalog.pg_database "
								  "WHERE datname='template0'");


	if (PQntuples(dbres) != 1)
		pg_fatal("template0 not found");

	locale = pg_malloc(sizeof(DbLocaleInfo));

	i_datencoding = PQfnumber(dbres, "encoding");
	i_datlocprovider = PQfnumber(dbres, "datlocprovider");
	i_datcollate = PQfnumber(dbres, "datcollate");
	i_datctype = PQfnumber(dbres, "datctype");
	i_datlocale = PQfnumber(dbres, "datlocale");

	locale->db_encoding = atoi(PQgetvalue(dbres, 0, i_datencoding));
	locale->db_collprovider = PQgetvalue(dbres, 0, i_datlocprovider)[0];
	locale->db_collate = pg_strdup(PQgetvalue(dbres, 0, i_datcollate));
	locale->db_ctype = pg_strdup(PQgetvalue(dbres, 0, i_datctype));
	if (PQgetisnull(dbres, 0, i_datlocale))
		locale->db_locale = NULL;
	else
		locale->db_locale = pg_strdup(PQgetvalue(dbres, 0, i_datlocale));

	cluster->template0 = locale;

	PQclear(dbres);
	PQfinish(conn);
}


/*
 * get_db_infos()
 *
 * Scans pg_database system catalog and populates all user
 * databases.
 */
static void
get_db_infos(ClusterInfo *cluster)
{
	PGconn	   *conn = connectToServer(cluster, "template1");
	PGresult   *res;
	int			ntups;
	int			tupnum;
	DbInfo	   *dbinfos;
	int			i_datname,
				i_oid,
				i_encoding,
				i_datcollate,
				i_datctype,
				i_datlocprovider,
				i_daticulocale,
				i_spclocation;
	char		query[QUERY_ALLOC];

	snprintf(query, sizeof(query),
			 "SELECT d.oid, d.datname, d.encoding, d.datcollate, d.datctype, ");
	if (GET_MAJOR_VERSION(cluster->major_version) >= 1700)
		snprintf(query + strlen(query), sizeof(query) - strlen(query),
				 "datlocprovider, datlocale, ");
	else if (GET_MAJOR_VERSION(cluster->major_version) >= 1500)
		snprintf(query + strlen(query), sizeof(query) - strlen(query),
				 "datlocprovider, daticulocale AS datlocale, ");
	else
		snprintf(query + strlen(query), sizeof(query) - strlen(query),
				 "'c' AS datlocprovider, NULL AS datlocale, ");
	snprintf(query + strlen(query), sizeof(query) - strlen(query),
			 "pg_catalog.pg_tablespace_location(t.oid) AS spclocation "
			 "FROM pg_catalog.pg_database d "
			 " LEFT OUTER JOIN pg_catalog.pg_tablespace t "
			 " ON d.dattablespace = t.oid "
			 "WHERE d.datallowconn = true "
	/* we don't preserve pg_database.oid so we sort by name */
			 "ORDER BY 2",
	/* 9.2 removed the spclocation column */
			 (GET_MAJOR_VERSION(cluster->major_version) == 803) ?
			 "t.spclocation" : "pg_catalog.pg_tablespace_location(t.oid)");

	res = executeQueryOrDie(conn, "%s", query);

	i_oid = PQfnumber(res, "oid");
	i_datname = PQfnumber(res, "datname");
	i_encoding = PQfnumber(res, "encoding");
	i_datcollate = PQfnumber(res, "datcollate");
	i_datctype = PQfnumber(res, "datctype");
	i_datlocprovider = PQfnumber(res, "datlocprovider");
	/* the query aliases the locale column as datlocale in every branch above */
	i_daticulocale = PQfnumber(res, "datlocale");
	i_spclocation = PQfnumber(res, "spclocation");

	ntups = PQntuples(res);
	dbinfos = (DbInfo *) pg_malloc0(sizeof(DbInfo) * ntups);

	for (tupnum = 0; tupnum < ntups; tupnum++)
	{
		dbinfos[tupnum].db_oid = atooid(PQgetvalue(res, tupnum, i_oid));
		dbinfos[tupnum].db_name = pg_strdup(PQgetvalue(res, tupnum, i_datname));
		/* GPDB: per-database locale/encoding for check_locale_and_encoding() */
		dbinfos[tupnum].db_encoding = atoi(PQgetvalue(res, tupnum, i_encoding));
		dbinfos[tupnum].db_collate = pg_strdup(PQgetvalue(res, tupnum, i_datcollate));
		dbinfos[tupnum].db_ctype = pg_strdup(PQgetvalue(res, tupnum, i_datctype));
		dbinfos[tupnum].db_collprovider = PQgetvalue(res, tupnum, i_datlocprovider)[0];
		if (PQgetisnull(res, tupnum, i_daticulocale))
			dbinfos[tupnum].db_iculocale = NULL;
		else
			dbinfos[tupnum].db_iculocale = pg_strdup(PQgetvalue(res, tupnum, i_daticulocale));
		snprintf(dbinfos[tupnum].db_tablespace, sizeof(dbinfos[tupnum].db_tablespace), "%s",
				 PQgetvalue(res, tupnum, i_spclocation));
	}
	PQclear(res);

	PQfinish(conn);

	cluster->dbarr.dbs = dbinfos;
	cluster->dbarr.ndbs = ntups;
}


/*
 * get_rel_infos_query()
 *
 * Returns the query for retrieving the relation information for all the user
 * tables and indexes in the database, for use by get_db_rel_and_slot_infos()'s
 * UpgradeTask.
 *
 * Note: the result is assumed to be sorted by OID.  This allows later
 * processing to match up old and new databases efficiently.
 */
static char *
get_rel_infos_query(ClusterInfo *cluster)
{
	PQExpBufferData query;

	initPQExpBuffer(&query);

	/*
	 * Create a CTE that collects OIDs of regular user tables and matviews,
	 * but excluding toast tables and indexes.  We assume that relations with
	 * OIDs >= FirstNormalObjectId belong to the user.  (That's probably
	 * redundant with the namespace-name exclusions, but let's be safe.)
	 *
	 * pg_largeobject contains user data that does not appear in pg_dump
	 * output, so we have to copy that system table.  It's easiest to do that
	 * by treating it as a user table.
	 */
	appendPQExpBuffer(&query,
					  "WITH regular_heap (reloid, indtable, toastheap) AS ( "
					  "  SELECT c.oid, 0::oid, 0::oid "
					  "  FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
					  "         ON c.relnamespace = n.oid "
					  "  WHERE relkind IN (" CppAsString2(RELKIND_RELATION) ", "
					  CppAsString2(RELKIND_AOSEGMENTS) ", "
					  CppAsString2(RELKIND_AOBLOCKDIR) ", "
					  CppAsString2(RELKIND_MATVIEW) "%s) AND "
	/* exclude possible orphaned temp tables */
					  "    ((n.nspname !~ '^pg_temp_' AND "
					  "      n.nspname !~ '^pg_toast_temp_' AND "
					  "      n.nspname NOT IN ('pg_catalog', 'information_schema', "
					  "                        'gp_toolkit', 'pg_bitmapindex', 'pg_aoseg', "
					  "                        'binary_upgrade', 'pg_toast') AND "
					  "      c.oid >= %u::pg_catalog.oid) OR "
					  "     (n.nspname = 'pg_catalog' AND "
					  "      relname IN ('pg_largeobject') ))), ",
					  (user_opts.transfer_mode == TRANSFER_MODE_SWAP) ?
					  ", " CppAsString2(RELKIND_SEQUENCE) : "",
					  FirstNormalObjectId);

	/*
	 * Add a CTE that collects OIDs of toast tables belonging to the tables
	 * selected by the regular_heap CTE.  (We have to do this separately
	 * because the namespace-name rules above don't work for toast tables.)
	 *
	 * GPDB: Starting GPDB7 CO tables no longer have TOAST tables. Hence,
	 * ignore toast OIDs for CO tables to avoid upgrade failures.
	 */
	appendPQExpBuffer(&query,
					  "  toast_heap (reloid, indtable, toastheap) AS ( "
					  "  SELECT c.reltoastrelid, 0::oid, c.oid "
					  "  FROM regular_heap JOIN pg_catalog.pg_class c "
					  "      ON regular_heap.reloid = c.oid "
					  "  WHERE c.reltoastrelid != 0%s), ",
					  (GET_MAJOR_VERSION(cluster->major_version) <= 904) ?
					  " AND c.relstorage <> 'c'" : "");

	/*
	 * Add a CTE that collects OIDs of all valid indexes on the previously
	 * selected tables.  We can ignore invalid indexes since pg_dump does.
	 * Testing indisready is necessary in 9.2, and harmless in earlier/later
	 * versions.
	 */
	appendPQExpBufferStr(&query,
						 "  all_index (reloid, indtable, toastheap) AS ( "
						 "  SELECT indexrelid, indrelid, 0::oid "
						 "  FROM pg_catalog.pg_index "
						 "  WHERE indisvalid AND indisready "
						 "    AND indrelid IN "
						 "        (SELECT reloid FROM regular_heap "
						 "         UNION ALL "
						 "         SELECT reloid FROM toast_heap)) ");

	/*
	 * And now we can write the query that retrieves the data we want for each
	 * heap and index relation.  Make sure result is sorted by OID.
	 */
	appendPQExpBuffer(&query,
					  "SELECT all_rels.*, n.nspname, c.relname, "
					  "  %s as relstorage, c.relkind, "
					  "  c.relfilenode, c.reltablespace, %s "
					  "FROM (SELECT * FROM regular_heap "
					  "      UNION ALL "
					  "      SELECT * FROM toast_heap "
					  "      UNION ALL "
					  "      SELECT * FROM all_index) all_rels "
					  "  JOIN pg_catalog.pg_class c "
					  "      ON all_rels.reloid = c.oid "
					  "  JOIN pg_catalog.pg_namespace n "
					  "     ON c.relnamespace = n.oid "
					  "  %s"
					  "  LEFT OUTER JOIN pg_catalog.pg_tablespace t "
					  "     ON c.reltablespace = t.oid "
					  "ORDER BY 1;",
	/*
	 * GPDB 7 with PostgreSQL v12 merge removed the relstorage column.
	 * It was replaced with the upstream 'relam'.
	 */
					  (GET_MAJOR_VERSION(cluster->major_version) <= 904) ?
					  "c.relstorage" :
					  "(CASE WHEN am.amname = 'ao_row' THEN 'a'"
					  " WHEN am.amname = 'ao_column' THEN 'c'"
					  " WHEN am.amname = 'heap' THEN 'h'"
					  " WHEN c.relkind = 'f' THEN 'x'"
					  " ELSE '' END)",

	/*
	 * 9.2 removed the spclocation column in upstream postgres, in GPDB it was
	 * removed in 6.0.0 during the 8.4 merge
	 */
					  (GET_MAJOR_VERSION(cluster->major_version) == 803) ?
					  "t.spclocation" : "pg_catalog.pg_tablespace_location(t.oid) AS spclocation",

					  (GET_MAJOR_VERSION(cluster->major_version) <= 1000) ?
					  "" : "LEFT OUTER JOIN pg_catalog.pg_am am ON c.relam = am.oid");

	return query.data;
}

/*
 * Callback function for processing results of the query returned by
 * get_rel_infos_query(), which is used for get_db_rel_and_slot_infos()'s
 * UpgradeTask.  This function stores the relation information for later use.
 */
static void
process_rel_infos(DbInfo *dbinfo, PGresult *res, void *arg)
{
	int			ntups = PQntuples(res);
	RelInfo    *relinfos = (RelInfo *) pg_malloc(sizeof(RelInfo) * ntups);
	int			i_reloid = PQfnumber(res, "reloid");
	int			i_indtable = PQfnumber(res, "indtable");
	int			i_toastheap = PQfnumber(res, "toastheap");
	int			i_nspname = PQfnumber(res, "nspname");
	int			i_relname = PQfnumber(res, "relname");
	int			i_relfilenumber = PQfnumber(res, "relfilenode");
	int			i_reltablespace = PQfnumber(res, "reltablespace");
	int			i_spclocation = PQfnumber(res, "spclocation");
	int			i_relstorage = PQfnumber(res, "relstorage");
	int			i_relkind = PQfnumber(res, "relkind");
	int			num_rels = 0;
	char	   *nspname = NULL;
	char	   *relname = NULL;
	char	   *tablespace = NULL;
	char	   *last_namespace = NULL;
	char	   *last_tablespace = NULL;
	char		relstorage;
	char		relkind;
	/*
	 * GPDB: gathering the append-only auxiliary catalog contents below requires
	 * a live connection to the database being processed, so open one for the
	 * cluster passed in as the task's callback argument.
	 */
	ClusterInfo *cluster = (ClusterInfo *) arg;
	PGconn	   *conn = connectToServer(cluster, dbinfo->db_name);

	AssertVariableIsOfType(&process_rel_infos, UpgradeTaskProcessCB);

	for (int relnum = 0; relnum < ntups; relnum++)
	{
		RelInfo    *curr = &relinfos[num_rels++];

		curr->reloid = atooid(PQgetvalue(res, relnum, i_reloid));

		if (!PQgetisnull(res, relnum, i_indtable))
		{
			curr->indtable = atooid(PQgetvalue(res, relnum, i_indtable));
		}
		else
		{
			curr->indtable = 0;
		}

		curr->toastheap = atooid(PQgetvalue(res, relnum, i_toastheap));

		nspname = PQgetvalue(res, relnum, i_nspname);
		curr->nsp_alloc = false;

		/*
		 * Many of the namespace and tablespace strings are identical, so we
		 * try to reuse the allocated string pointers where possible to reduce
		 * memory consumption.
		 */
		/* Can we reuse the previous string allocation? */
		if (last_namespace && strcmp(nspname, last_namespace) == 0)
			curr->nspname = last_namespace;
		else
		{
			last_namespace = curr->nspname = pg_strdup(nspname);
			curr->nsp_alloc = true;
		}

		relname = PQgetvalue(res, relnum, i_relname);
		curr->relname = pg_strdup(relname);

		curr->relfilenumber = atooid(PQgetvalue(res, relnum, i_relfilenumber));
		curr->tblsp_alloc = false;

		/* Is the tablespace oid non-default? */
		if (atooid(PQgetvalue(res, relnum, i_reltablespace)) != 0)
		{
			/*
			 * The tablespace location might be "", meaning the cluster
			 * default location, i.e. pg_default or pg_global.
			 */
			tablespace = PQgetvalue(res, relnum, i_spclocation);

			/* Can we reuse the previous string allocation? */
			if (last_tablespace && strcmp(tablespace, last_tablespace) == 0)
				curr->tablespace = last_tablespace;
			else
			{
				last_tablespace = curr->tablespace = pg_strdup(tablespace);
				curr->tblsp_alloc = true;
			}
		}
		else
			/* A zero reltablespace oid indicates the database tablespace. */
			curr->tablespace = dbinfo->db_tablespace;

		/* Collect extra information about append-only tables */
		relstorage = PQgetvalue(res, relnum, i_relstorage) [0];
		curr->relstorage = relstorage;

		relkind = PQgetvalue(res, relnum, i_relkind) [0];

		/*
		 * The structure of append
		 * optimized tables is similar enough for row and column oriented
		 * tables so we can handle them both here.
		 */
		if (is_appendonly(relstorage))
		{
			char	   *segrel;
			char	   *visimaprel;
			char	   *blkdirrel = NULL;
			PGresult   *aores;
			int			j;

			/*
			 * First query the catalog for the auxiliary heap relations which
			 * describe AO{CS} relations. The segrel and visimap must exist
			 * but the blkdirrel is created when required so it might not
			 * exist.
			 *
			 * We don't dump the block directory, even if it exists, if the
			 * table doesn't have any indexes. This isn't just an optimization:
			 * restoring it wouldn't work, because without indexes, restore
			 * won't create a block directory in the new cluster.
			 */
			aores = executeQueryOrDie(conn,
					 "SELECT cs.relname AS segrel, "
					 "       cv.relname AS visimaprel, "
					 "       cb.relname AS blkdirrel "
					 "FROM   pg_appendonly a "
					 "       JOIN pg_class cs on (cs.oid = a.segrelid) "
					 "       JOIN pg_class cv on (cv.oid = a.visimaprelid) "
					 "       LEFT JOIN pg_class cb on (cb.oid = a.blkdirrelid "
					 "                                 AND a.blkdirrelid <> 0 "
					 "                                 AND EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.relid)) "
					 "WHERE  a.relid = %u::pg_catalog.oid ",
					 curr->reloid);

			if (PQntuples(aores) == 0)
				pg_log(PG_FATAL, "Unable to find auxiliary AO relations for %u (%s)\n",
					   curr->reloid, curr->relname);

			segrel = pg_strdup(PQgetvalue(aores, 0, PQfnumber(aores, "segrel")));
			visimaprel = pg_strdup(PQgetvalue(aores, 0, PQfnumber(aores, "visimaprel")));
			if (!PQgetisnull(aores, 0, PQfnumber(aores, "blkdirrel")))
				blkdirrel = pg_strdup(PQgetvalue(aores, 0, PQfnumber(aores, "blkdirrel")));

			PQclear(aores);

			if (relstorage == 'a')
			{
				aores = executeQueryOrDie(conn,
							"SELECT segno, eof, tupcount, varblockcount, "
							"       eofuncompressed, modcount, state, "
							"       formatversion "
							"FROM   pg_aoseg.%s",
							segrel);

				curr->naosegments = PQntuples(aores);
				curr->aosegments = (AOSegInfo *) pg_malloc(sizeof(AOSegInfo) * curr->naosegments);

				for (j = 0; j < curr->naosegments; j++)
				{
					AOSegInfo *aoseg = &curr->aosegments[j];

					aoseg->segno = atoi(PQgetvalue(aores, j, PQfnumber(aores, "segno")));
					aoseg->eof = atoll(PQgetvalue(aores, j, PQfnumber(aores, "eof")));
					aoseg->tupcount = atoll(PQgetvalue(aores, j, PQfnumber(aores, "tupcount")));
					aoseg->varblockcount = atoll(PQgetvalue(aores, j, PQfnumber(aores, "varblockcount")));
					aoseg->eofuncompressed = atoll(PQgetvalue(aores, j, PQfnumber(aores, "eofuncompressed")));
					aoseg->modcount = atoll(PQgetvalue(aores, j, PQfnumber(aores, "modcount")));
					aoseg->state = atoi(PQgetvalue(aores, j, PQfnumber(aores, "state")));
					aoseg->version = atoi(PQgetvalue(aores, j, PQfnumber(aores, "formatversion")));
				}

				PQclear(aores);
			}
			else
			{
				aores = executeQueryOrDie(conn,
							"SELECT segno, tupcount, varblockcount, vpinfo, "
							"       modcount, formatversion, state "
							"FROM   pg_aoseg.%s",
							segrel);

				curr->naosegments = PQntuples(aores);
				curr->aocssegments = (AOCSSegInfo *) pg_malloc(sizeof(AOCSSegInfo) * curr->naosegments);

				for (j = 0; j < curr->naosegments; j++)
				{
					AOCSSegInfo *aocsseg = &curr->aocssegments[j];

					aocsseg->segno = atoi(PQgetvalue(aores, j, PQfnumber(aores, "segno")));
					aocsseg->tupcount = atoll(PQgetvalue(aores, j, PQfnumber(aores, "tupcount")));
					aocsseg->varblockcount = atoll(PQgetvalue(aores, j, PQfnumber(aores, "varblockcount")));
					aocsseg->vpinfo = pg_strdup(PQgetvalue(aores, j, PQfnumber(aores, "vpinfo")));
					aocsseg->modcount = atoll(PQgetvalue(aores, j, PQfnumber(aores, "modcount")));
					aocsseg->state = atoi(PQgetvalue(aores, j, PQfnumber(aores, "state")));
					aocsseg->version = atoi(PQgetvalue(aores, j, PQfnumber(aores, "formatversion")));
				}

				PQclear(aores);
			}

			aores = executeQueryOrDie(conn,
						"SELECT segno, first_row_no, visimap "
						"FROM pg_aoseg.%s",
						visimaprel);

			curr->naovisimaps = PQntuples(aores);
			curr->aovisimaps = (AOVisiMapInfo *) pg_malloc(sizeof(AOVisiMapInfo) * curr->naovisimaps);

			for (j = 0; j < curr->naovisimaps; j++)
			{
				AOVisiMapInfo *aovisimap = &curr->aovisimaps[j];

				aovisimap->segno = atoi(PQgetvalue(aores, j, PQfnumber(aores, "segno")));
				aovisimap->first_row_no = atoll(PQgetvalue(aores, j, PQfnumber(aores, "first_row_no")));
				aovisimap->visimap = pg_strdup(PQgetvalue(aores, j, PQfnumber(aores, "visimap")));
			}

			PQclear(aores);

			/*
			 * Get contents of pg_aoblkdir_<oid>. If pg_appendonly.blkdirrelid
			 * is InvalidOid then there is no blkdir table.
			 */
			if (blkdirrel)
			{
				aores = executeQueryOrDie(conn,
							"SELECT segno, columngroup_no, first_row_no, minipage "
							"FROM pg_aoseg.%s",
							blkdirrel);

				curr->naoblkdirs = PQntuples(aores);
				curr->aoblkdirs = (AOBlkDir *) pg_malloc(sizeof(AOBlkDir) * curr->naoblkdirs);

				for (j = 0; j < curr->naoblkdirs; j++)
				{
					AOBlkDir *aoblkdir = &curr->aoblkdirs[j];

					aoblkdir->segno = atoi(PQgetvalue(aores, j, PQfnumber(aores, "segno")));
					aoblkdir->columngroup_no = atoi(PQgetvalue(aores, j, PQfnumber(aores, "columngroup_no")));
					aoblkdir->first_row_no = atoll(PQgetvalue(aores, j, PQfnumber(aores, "first_row_no")));
					aoblkdir->minipage = pg_strdup(PQgetvalue(aores, j, PQfnumber(aores, "minipage")));
				}

				PQclear(aores);
			}
			else
			{
				curr->aoblkdirs = NULL;
				curr->naoblkdirs = 0;
			}

			pg_free(segrel);
			pg_free(visimaprel);
			pg_free(blkdirrel);
		}
		else
		{
			/* Not an AO/AOCS relation */
			curr->aosegments = NULL;
			curr->aocssegments = NULL;
			curr->naosegments = 0;
			curr->aovisimaps = NULL;
			curr->naovisimaps = 0;
			curr->naoblkdirs = 0;
			curr->aoblkdirs = NULL;
		}
	}

	PQfinish(conn);

	dbinfo->rel_arr.rels = relinfos;
	dbinfo->rel_arr.nrels = num_rels;
}

/*
 * get_old_cluster_logical_slot_infos_query()
 *
 * Returns the query for retrieving the logical slot information for all the
 * logical replication slots in the database, for use by
 * get_db_rel_and_slot_infos()'s UpgradeTask.  The status of each logical slot
 * is checked in check_old_cluster_for_valid_slots().
 */
static char *
get_old_cluster_logical_slot_infos_query(void)
{
	/*
	 * Fetch the logical replication slot information. The check whether the
	 * slot is considered caught up is done by an upgrade function. This
	 * regards the slot as caught up if we don't find any decodable changes.
	 * See binary_upgrade_logical_slot_has_caught_up().
	 *
	 * Note that we can't ensure whether the slot is caught up during
	 * live_check as the new WAL records could be generated.
	 *
	 * We intentionally skip checking the WALs for invalidated slots as the
	 * corresponding WALs could have been removed for such slots.
	 *
	 * The temporary slots are explicitly ignored while checking because such
	 * slots cannot exist after the upgrade. During the upgrade, clusters are
	 * started and stopped several times causing any temporary slots to be
	 * removed.
	 */
	return psprintf("SELECT slot_name, plugin, two_phase, failover, "
					"%s as caught_up, invalidation_reason IS NOT NULL as invalid "
					"FROM pg_catalog.pg_replication_slots "
					"WHERE slot_type = 'logical' AND "
					"database = current_database() AND "
					"temporary IS FALSE;",
					user_opts.live_check ? "FALSE" :
					"(CASE WHEN invalidation_reason IS NOT NULL THEN FALSE "
					"ELSE (SELECT pg_catalog.binary_upgrade_logical_slot_has_caught_up(slot_name)) "
					"END)");
}

/*
 * Callback function for processing results of the query returned by
 * get_old_cluster_logical_slot_infos_query(), which is used for
 * get_db_rel_and_slot_infos()'s UpgradeTask.  This function stores the logical
 * slot information for later use.
 */
static void
process_old_cluster_logical_slot_infos(DbInfo *dbinfo, PGresult *res, void *arg)
{
	LogicalSlotInfo *slotinfos = NULL;
	int			num_slots = PQntuples(res);

	AssertVariableIsOfType(&process_old_cluster_logical_slot_infos,
						   UpgradeTaskProcessCB);

	if (num_slots)
	{
		int			i_slotname;
		int			i_plugin;
		int			i_twophase;
		int			i_failover;
		int			i_caught_up;
		int			i_invalid;

		slotinfos = (LogicalSlotInfo *) pg_malloc(sizeof(LogicalSlotInfo) * num_slots);

		i_slotname = PQfnumber(res, "slot_name");
		i_plugin = PQfnumber(res, "plugin");
		i_twophase = PQfnumber(res, "two_phase");
		i_failover = PQfnumber(res, "failover");
		i_caught_up = PQfnumber(res, "caught_up");
		i_invalid = PQfnumber(res, "invalid");

		for (int slotnum = 0; slotnum < num_slots; slotnum++)
		{
			LogicalSlotInfo *curr = &slotinfos[slotnum];

			curr->slotname = pg_strdup(PQgetvalue(res, slotnum, i_slotname));
			curr->plugin = pg_strdup(PQgetvalue(res, slotnum, i_plugin));
			curr->two_phase = (strcmp(PQgetvalue(res, slotnum, i_twophase), "t") == 0);
			curr->failover = (strcmp(PQgetvalue(res, slotnum, i_failover), "t") == 0);
			curr->caught_up = (strcmp(PQgetvalue(res, slotnum, i_caught_up), "t") == 0);
			curr->invalid = (strcmp(PQgetvalue(res, slotnum, i_invalid), "t") == 0);
		}
	}

	dbinfo->slot_arr.slots = slotinfos;
	dbinfo->slot_arr.nslots = num_slots;
}


/*
 * count_old_cluster_logical_slots()
 *
 * Returns the number of logical replication slots for all databases.
 *
 * Note: this function always returns 0 if the old_cluster is PG16 and prior
 * because we gather slot information only for cluster versions greater than or
 * equal to PG17. See get_old_cluster_logical_slot_infos().
 */
int
count_old_cluster_logical_slots(void)
{
	int			slot_count = 0;

	for (int dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
		slot_count += old_cluster.dbarr.dbs[dbnum].slot_arr.nslots;

	return slot_count;
}

/*
 * get_subscription_count()
 *
 * Gets the number of subscriptions in the cluster.
 */
void
get_subscription_count(ClusterInfo *cluster)
{
	PGconn	   *conn;
	PGresult   *res;

	conn = connectToServer(cluster, "template1");
	res = executeQueryOrDie(conn, "SELECT count(*) "
							"FROM pg_catalog.pg_subscription");
	cluster->nsubs = atoi(PQgetvalue(res, 0, 0));

	PQclear(res);
	PQfinish(conn);
}

static void
free_db_and_rel_infos(DbInfoArr *db_arr)
{
	int			dbnum;

	for (dbnum = 0; dbnum < db_arr->ndbs; dbnum++)
	{
		free_rel_infos(&db_arr->dbs[dbnum].rel_arr);
		pg_free(db_arr->dbs[dbnum].db_name);
	}
	pg_free(db_arr->dbs);
	db_arr->dbs = NULL;
	db_arr->ndbs = 0;
}


static void
free_rel_infos(RelInfoArr *rel_arr)
{
	int			relnum;

	for (relnum = 0; relnum < rel_arr->nrels; relnum++)
	{
		if (rel_arr->rels[relnum].nsp_alloc)
			pg_free(rel_arr->rels[relnum].nspname);
		pg_free(rel_arr->rels[relnum].relname);
		if (rel_arr->rels[relnum].tblsp_alloc)
			pg_free(rel_arr->rels[relnum].tablespace);
	}
	pg_free(rel_arr->rels);
	rel_arr->nrels = 0;
}


static void
print_db_infos(DbInfoArr *db_arr)
{
	int			dbnum;

	for (dbnum = 0; dbnum < db_arr->ndbs; dbnum++)
	{
		DbInfo	   *pDbInfo = &db_arr->dbs[dbnum];

		pg_log(PG_VERBOSE, "Database: \"%s\"", pDbInfo->db_name);
		print_rel_infos(&pDbInfo->rel_arr);
		print_slot_infos(&pDbInfo->slot_arr);
	}
}


static void
print_rel_infos(RelInfoArr *rel_arr)
{
	int			relnum;

	for (relnum = 0; relnum < rel_arr->nrels; relnum++)
		pg_log(PG_VERBOSE, "relname: \"%s.%s\", reloid: %u, reltblspace: \"%s\"",
			   rel_arr->rels[relnum].nspname,
			   rel_arr->rels[relnum].relname,
			   rel_arr->rels[relnum].reloid,
			   rel_arr->rels[relnum].tablespace);
}

static void
print_slot_infos(LogicalSlotInfoArr *slot_arr)
{
	/* Quick return if there are no logical slots. */
	if (slot_arr->nslots == 0)
		return;

	pg_log(PG_VERBOSE, "Logical replication slots in the database:");

	for (int slotnum = 0; slotnum < slot_arr->nslots; slotnum++)
	{
		LogicalSlotInfo *slot_info = &slot_arr->slots[slotnum];

		pg_log(PG_VERBOSE, "slot name: \"%s\", output plugin: \"%s\", two_phase: %s",
			   slot_info->slotname,
			   slot_info->plugin,
			   slot_info->two_phase ? "true" : "false");
	}
}
