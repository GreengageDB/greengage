/*-------------------------------------------------------------------------
 *
 * segadmin.c
 *	  Functions to support administrative tasks with GPDB segments.
 *
 * Portions Copyright (c) 2010 Greenplum
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/utils/gp/segadmin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "libpq-fe.h"
#include "pqexpbuffer.h"

#include "catalog/gp_segment_config.h"
#include "catalog/pg_proc.h"
#include "catalog/indexing.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbfts.h"
#include "postmaster/startup.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#define MASTER_ONLY 0x1
#define UTILITY_MODE 0x2
#define SUPERUSER 0x4
#define READ_ONLY 0x8
#define SEGMENT_ONLY 0x10
#define STANDBY_ONLY 0x20
#define SINGLE_USER_MODE 0x40

/* look up a particular segment */
static GpSegConfigEntry *
get_segconfig(int16 dbid)
{
	return dbid_get_dbinfo(dbid);
}

/* Convenience routine to look up the mirror for a given segment index */
static int16
content_get_mirror_dbid(int16 contentid)
{
	return contentid_get_dbid(contentid, GP_SEGMENT_CONFIGURATION_ROLE_MIRROR, false /* false == current, not
							    * preferred, role */ );
}

/* Tell the caller whether a mirror exists at a given segment index */
static bool
segment_has_mirror(int16 contentid)
{
	return content_get_mirror_dbid(contentid) != 0;
}

/*
 * As the function name says, test whether a given dbid is the dbid of the
 * standby master.
 */
static bool
dbid_is_master_standby(int16 dbid)
{
	int16		standbydbid = content_get_mirror_dbid(MASTER_CONTENT_ID);

	return (standbydbid == dbid);
}

/*
 * Tell the caller whether a standby master is defined in the system.
 */
static bool
standby_exists()
{
	return segment_has_mirror(MASTER_CONTENT_ID);
}

/*
 * Get the highest dbid defined in the system. We AccessExclusiveLock
 * gp_segment_configuration to prevent races but no one should be calling
 * this code concurrently if we've done our job right.
 */
static int16
get_maxdbid()
{
	Relation	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	int16		dbid = 0;
	HeapTuple	tuple;
	SysScanDesc sscan;

	sscan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		dbid = Max(dbid,
				   ((Form_gp_segment_configuration) GETSTRUCT(tuple))->dbid);
	}
	systable_endscan(sscan);
	heap_close(rel, NoLock);

	return dbid;
}

/**
 * Get an available dbid value. We AccessExclusiveLock
 * gp_segment_configuration to prevent races but no one should be calling
 * this code concurrently if we've done our job right.
 */
static int16
get_availableDbId()
{
	/*
	 * Set up hash of used dbids.  We use int32 here because int16 doesn't
	 * have a convenient hash and we can use casting below to check for
	 * overflow of int16
	 */
	HASHCTL		hash_ctl;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int32);
	hash_ctl.entrysize = sizeof(int32);
	hash_ctl.hash = int32_hash;
	HTAB	   *htab = hash_create("Temporary table of dbids",
								   1024,
								   &hash_ctl,
								   HASH_ELEM | HASH_FUNCTION);

	/* scan GpSegmentConfigRelationId */
	Relation	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	HeapTuple	tuple;
	SysScanDesc sscan;

	sscan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		int32		dbid = (int32) ((Form_gp_segment_configuration) GETSTRUCT(tuple))->dbid;

		(void) hash_search(htab, (void *) &dbid, HASH_ENTER, NULL);
	}
	systable_endscan(sscan);

	heap_close(rel, NoLock);

	/* search for available dbid */
	for (int32 dbid = 1;; dbid++)
	{
		if (dbid != (int16) dbid)
			elog(ERROR, "unable to find available dbid");

		if (hash_search(htab, (void *) &dbid, HASH_FIND, NULL) == NULL)
		{
			hash_destroy(htab);
			return dbid;
		}
	}
}


/*
 * Get the highest contentid defined in the system. As above, we
 * AccessExclusiveLock gp_segment_configuration to prevent races but no
 * one should be calling this code concurrently if we've done our job right.
 */
static int16
get_maxcontentid()
{
	Relation	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	int16		contentid = 0;
	HeapTuple	tuple;
	SysScanDesc sscan;

	sscan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		contentid = Max(contentid,
						((Form_gp_segment_configuration) GETSTRUCT(tuple))->content);
	}
	systable_endscan(sscan);
	heap_close(rel, NoLock);

	return contentid;
}

/*
 * Check that the code is being called in right context.
 */
static void
mirroring_sanity_check(int flags, const char *func)
{
	if ((flags & MASTER_ONLY) == MASTER_ONLY)
	{
		if (GpIdentity.dbid == UNINITIALIZED_GP_IDENTITY_VALUE)
			elog(ERROR, "%s requires valid GpIdentity dbid", func);

		if (!IS_QUERY_DISPATCHER())
			elog(ERROR, "%s must be run on the master", func);
	}

	if ((flags & UTILITY_MODE) == UTILITY_MODE)
	{
		if (Gp_role != GP_ROLE_UTILITY)
			elog(ERROR, "%s must be run in utility mode", func);
	}

	if ((flags & SINGLE_USER_MODE) == SINGLE_USER_MODE)
	{
		if (IsUnderPostmaster)
			elog(ERROR, "%s must be run in single-user mode", func);
	}

	if ((flags & SUPERUSER) == SUPERUSER)
	{
		if (!superuser())
			elog(ERROR, "%s can only be run by a superuser", func);
	}

	if ((flags & SEGMENT_ONLY) == SEGMENT_ONLY)
	{
		if (GpIdentity.dbid == UNINITIALIZED_GP_IDENTITY_VALUE)
			elog(ERROR, "%s requires valid GpIdentity dbid", func);

		if (IS_QUERY_DISPATCHER())
			elog(ERROR, "%s cannot be run on the master", func);
	}

	if ((flags & STANDBY_ONLY) == STANDBY_ONLY)
	{
		if (GpIdentity.dbid == UNINITIALIZED_GP_IDENTITY_VALUE)
			elog(ERROR, "%s requires valid GpIdentity dbid", func);

		if (!dbid_is_master_standby(GpIdentity.dbid))
			elog(ERROR, "%s can only be run on the standby master", func);
	}
}

/*
 * Add a new row to gp_segment_configuration.
 */
static void
add_segment_config(GpSegConfigEntry *i)
{
	Relation	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	Datum		values[Natts_gp_segment_configuration];
	bool		nulls[Natts_gp_segment_configuration];
	HeapTuple	tuple;

	MemSet(nulls, false, sizeof(nulls));

	values[Anum_gp_segment_configuration_dbid - 1] = Int16GetDatum(i->dbid);
	values[Anum_gp_segment_configuration_content - 1] = Int16GetDatum(i->segindex);
	values[Anum_gp_segment_configuration_role - 1] = CharGetDatum(i->role);
	values[Anum_gp_segment_configuration_preferred_role - 1] =
		CharGetDatum(i->preferred_role);
	values[Anum_gp_segment_configuration_mode - 1] =
		CharGetDatum(i->mode);
	values[Anum_gp_segment_configuration_status - 1] =
		CharGetDatum(i->status);
	values[Anum_gp_segment_configuration_port - 1] =
		Int32GetDatum(i->port);
	values[Anum_gp_segment_configuration_hostname - 1] =
		CStringGetTextDatum(i->hostname);
	values[Anum_gp_segment_configuration_address - 1] =
		CStringGetTextDatum(i->address);
	values[Anum_gp_segment_configuration_datadir - 1] =
		CStringGetTextDatum(i->datadir);

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* insert a new tuple */
	CatalogTupleInsert(rel, tuple);

	heap_close(rel, NoLock);
}

/*
 * Master function for adding a new segment
 */
static void
add_segment_config_entry(GpSegConfigEntry *i)
{
	/* Add gp_segment_configuration entry */
	add_segment_config(i);
}

/*
 * Remove a gp_segment_configuration entry
 */
static void
remove_segment_config(int16 dbid)
{
	int			numDel = 0;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple	tuple;
	Relation	rel;

	rel = heap_open(GpSegmentConfigRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_gp_segment_configuration_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(dbid));

	sscan = systable_beginscan(rel, GpSegmentConfigDbidIndexId, true,
							   NULL, 1, &scankey);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		CatalogTupleDelete(rel, &tuple->t_self);
		numDel++;
	}
	systable_endscan(sscan);

	Assert(numDel > 0);

	heap_close(rel, NoLock);
}

static void
add_segment(GpSegConfigEntry *new_segment_information)
{
	int16		primary_dbid = new_segment_information->dbid;

	if (new_segment_information->role == GP_SEGMENT_CONFIGURATION_ROLE_MIRROR)
	{
		primary_dbid = contentid_get_dbid(new_segment_information->segindex, GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, false	/* false == current, not
										    * preferred, role */ );
		if (!primary_dbid)
			elog(ERROR, "contentid %i does not point to an existing segment",
				 new_segment_information->segindex);

		/*
		 * no mirrors should be defined
		 */
		if (segment_has_mirror(new_segment_information->segindex))
			elog(ERROR, "segment already has a mirror defined");

		/*
		 * figure out if the preferred role of this mirror needs to be primary
		 * or mirror (no preferred primary -- make this one the preferred
		 * primary)
		 */
		int			preferredPrimaryDbId = contentid_get_dbid(new_segment_information->segindex, GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, true /* preferred role */ );

		if (preferredPrimaryDbId == 0 && new_segment_information->preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_MIRROR)
		{
			elog(NOTICE, "override preferred_role of this mirror as primary to support rebalance operation.");
			new_segment_information->preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
		}
	}

	add_segment_config_entry(new_segment_information);
}

/*
 * Tell the master about a new primary segment.
 *
 * gp_add_segment_primary(hostname, address, port)
 *
 * Args:
 *   hostname - host name string
 *   address - either hostname or something else
 *   port - port number
 *   datadir - absolute path to primary data directory.
 *
 * Returns the dbid of the new segment.
 */
Datum
gp_add_segment_primary(PG_FUNCTION_ARGS)
{
	GpSegConfigEntry	new;

	MemSet(&new, 0, sizeof(GpSegConfigEntry));

	if (PG_ARGISNULL(0))
		elog(ERROR, "hostname cannot be NULL");
	new.hostname = TextDatumGetCString(PG_GETARG_DATUM(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "address cannot be NULL");
	new.address = TextDatumGetCString(PG_GETARG_DATUM(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "port cannot be NULL");
	new.port = PG_GETARG_INT32(2);

	if (PG_ARGISNULL(3))
		elog(ERROR, "datadir cannot be NULL");
	new.datadir = TextDatumGetCString(PG_GETARG_DATUM(3));

	mirroring_sanity_check(MASTER_ONLY | SUPERUSER, "gp_add_segment_primary");

	new.segindex = get_maxcontentid() + 1;
	new.dbid = get_availableDbId();
	new.role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
	new.preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
	new.mode = GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC;
	new.status = GP_SEGMENT_CONFIGURATION_STATUS_UP;

	add_segment(&new);

	PG_RETURN_INT16(new.dbid);
}

/*
 * Currently this is called by `gpinitsystem`.
 *
 * This method shouldn't be called at all. `gp_add_segment_primary()` and
 * `gp_add_segment_mirror()` should be used instead. This is to avoid setting
 * character values of role, preferred_role, mode, status, etc. outside database.
 */
Datum
gp_add_segment(PG_FUNCTION_ARGS)
{
	GpSegConfigEntry new;

	MemSet(&new, 0, sizeof(GpSegConfigEntry));

	if (PG_ARGISNULL(0))
		elog(ERROR, "dbid cannot be NULL");
	new.dbid = PG_GETARG_INT16(0);

	if (PG_ARGISNULL(1))
		elog(ERROR, "content cannot be NULL");
	new.segindex = PG_GETARG_INT16(1);

	if (PG_ARGISNULL(2))
		elog(ERROR, "role cannot be NULL");
	new.role = PG_GETARG_CHAR(2);

	if (PG_ARGISNULL(3))
		elog(ERROR, "preferred_role cannot be NULL");
	new.preferred_role = PG_GETARG_CHAR(3);

	if (PG_ARGISNULL(4))
		elog(ERROR, "mode cannot be NULL");
	new.mode = PG_GETARG_CHAR(4);

	if (PG_ARGISNULL(5))
		elog(ERROR, "status cannot be NULL");
	new.status = PG_GETARG_CHAR(5);

	if (PG_ARGISNULL(6))
		elog(ERROR, "port cannot be NULL");
	new.port = PG_GETARG_INT32(6);

	if (PG_ARGISNULL(7))
		elog(ERROR, "hostname cannot be NULL");
	new.hostname = TextDatumGetCString(PG_GETARG_DATUM(7));

	if (PG_ARGISNULL(8))
		elog(ERROR, "address cannot be NULL");
	new.address = TextDatumGetCString(PG_GETARG_DATUM(8));

	if (PG_ARGISNULL(9))
		elog(ERROR, "datadir cannot be NULL");
	new.datadir = TextDatumGetCString(PG_GETARG_DATUM(9));

	mirroring_sanity_check(MASTER_ONLY | SUPERUSER, "gp_add_segment");

	new.mode = GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC;
	elog(NOTICE, "mode is changed to GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC under walrep.");

	add_segment(&new);

	PG_RETURN_INT16(new.dbid);
}

/*
 * Master function to remove a segment from all catalogs
 */
static void
remove_segment(int16 pridbid, int16 mirdbid)
{
	/* Check that we're removing a mirror, not a primary */
	get_segconfig(mirdbid);

	remove_segment_config(mirdbid);
}

/*
 * Remove knowledge of a segment from the master.
 *
 * gp_remove_segment(dbid)
 *
 * Args:
 *   dbid - db identifier
 *
 * Returns:
 *   true on success, otherwise error.
 */
Datum
gp_remove_segment(PG_FUNCTION_ARGS)
{
	int16		dbid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "dbid cannot be NULL");

	dbid = PG_GETARG_INT16(0);

	mirroring_sanity_check(MASTER_ONLY | SUPERUSER | UTILITY_MODE,
						   "gp_remove_segment");
	remove_segment(dbid, dbid);

	PG_RETURN_BOOL(true);
}

/*
 * Add a mirror of an existing segment.
 *
 * gp_add_segment_mirror(contentid, hostname, address, port, datadir)
 */
Datum
gp_add_segment_mirror(PG_FUNCTION_ARGS)
{
	GpSegConfigEntry new;

	if (PG_ARGISNULL(0))
		elog(ERROR, "contentid cannot be NULL");
	new.segindex = PG_GETARG_INT16(0);

	if (PG_ARGISNULL(1))
		elog(ERROR, "hostname cannot be NULL");
	new.hostname = TextDatumGetCString(PG_GETARG_DATUM(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "address cannot be NULL");
	new.address = TextDatumGetCString(PG_GETARG_DATUM(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "port cannot be NULL");
	new.port = PG_GETARG_INT32(3);

	if (PG_ARGISNULL(4))
		elog(ERROR, "datadir cannot be NULL");
	new.datadir = TextDatumGetCString(PG_GETARG_DATUM(4));
	
	mirroring_sanity_check(MASTER_ONLY | SUPERUSER, "gp_add_segment_mirror");

	new.dbid = get_availableDbId();
	new.mode = GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC;
	new.status = GP_SEGMENT_CONFIGURATION_STATUS_DOWN;
	new.role = GP_SEGMENT_CONFIGURATION_ROLE_MIRROR;
	new.preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_MIRROR;

	add_segment(&new);

	PG_RETURN_INT16(new.dbid);
}

/*
 * Remove a segment mirror.
 *
 * gp_remove_segment_mirror(contentid)
 *
 * Args:
 *   contentid - segment index at which to remove the mirror
 *
 * Returns:
 *   true upon success, otherwise throws error.
 */
Datum
gp_remove_segment_mirror(PG_FUNCTION_ARGS)
{
	int16		contentid = 0;
	Relation	rel;
	int16		pridbid;
	int16		mirdbid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "dbid cannot be NULL");
	contentid = PG_GETARG_INT16(0);

	mirroring_sanity_check(MASTER_ONLY | SUPERUSER, "gp_remove_segment_mirror");

	/* avoid races */
	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);

	pridbid = contentid_get_dbid(contentid, GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, false /* false == current, not
								   * preferred, role */ );

	if (!pridbid)
		elog(ERROR, "no dbid for contentid %i", contentid);

	if (!segment_has_mirror(contentid))
		elog(ERROR, "segment does not have a mirror");

	mirdbid = contentid_get_dbid(contentid, GP_SEGMENT_CONFIGURATION_ROLE_MIRROR, false	/* false == current, not
								   * preferred, role */ );
	if (!mirdbid)
		elog(ERROR, "no mirror dbid for contentid %i", contentid);

	remove_segment(pridbid, mirdbid);

	heap_close(rel, NoLock);

	PG_RETURN_BOOL(true);
}

/*
 * Add a master standby.
 *
 * gp_add_master_standby(hostname, address, [port])
 *
 * Args:
 *  hostname - as above
 *  address - as above
 *  port - the port number of new standby
 *
 * Returns:
 *  dbid of the new standby
 */
Datum
gp_add_master_standby_port(PG_FUNCTION_ARGS)
{
	return gp_add_master_standby(fcinfo);
}

Datum
gp_add_master_standby(PG_FUNCTION_ARGS)
{
	int			maxdbid;
	int16		master_dbid;
	Relation	gprel;
	GpSegConfigEntry	*config;

	if (PG_ARGISNULL(0))
		elog(ERROR, "host name cannot be NULL");
	if (PG_ARGISNULL(1))
		elog(ERROR, "address cannot be NULL");
	if (PG_ARGISNULL(2))
		elog(ERROR, "datadir cannot be NULL");

	mirroring_sanity_check(MASTER_ONLY | UTILITY_MODE,
						   "gp_add_master_standby");

	/* Check if the system is ok */
	if (standby_exists())
		elog(ERROR, "only a single master standby may be defined");

	/* Lock exclusively to avoid concurrent changes */
	gprel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);

	maxdbid = get_maxdbid();

	/*
	 * Don't reference GpIdentity.dbid, as it is legitimate to set -1 for -b
	 * option in utility mode.  Content ID = -1 AND role =
	 * GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY is the definition of primary
	 * master.
	 */
	master_dbid = contentid_get_dbid(MASTER_CONTENT_ID,
									 GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, false);
	config = get_segconfig(master_dbid);

	config->dbid = maxdbid + 1;
	config->role = GP_SEGMENT_CONFIGURATION_ROLE_MIRROR;
	config->preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_MIRROR;
	config->mode = GP_SEGMENT_CONFIGURATION_MODE_INSYNC;
	config->status = GP_SEGMENT_CONFIGURATION_STATUS_UP;

	config->hostname = TextDatumGetCString(PG_GETARG_TEXT_P(0));

	config->address = TextDatumGetCString(PG_GETARG_TEXT_P(1));

	config->datadir = TextDatumGetCString(PG_GETARG_TEXT_P(2));
	
	/* Use the new port number if specified */
	if (PG_NARGS() > 3 && !PG_ARGISNULL(3))
		config->port = PG_GETARG_INT32(3);

	add_segment_config_entry(config);
	
	heap_close(gprel, NoLock);

	PG_RETURN_INT16(config->dbid);
}

/*
 * Remove the master standby.
 *
 * gp_remove_master_standby()
 *
 * Returns:
 *  true upon success otherwise false
 */
Datum
gp_remove_master_standby(PG_FUNCTION_ARGS)
{
	int16		dbid = master_standby_dbid();

	mirroring_sanity_check(SUPERUSER | MASTER_ONLY | UTILITY_MODE,
						   "gp_remove_master_standby");

	if (!dbid)
		elog(ERROR, "no master standby defined");

	remove_segment(GpIdentity.dbid, dbid);

	PG_RETURN_BOOL(true);
}

static void
segment_config_activate_standby(int16 standby_dbid, int16 master_dbid)
{
	/* we use AccessExclusiveLock to prevent races */
	Relation	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	HeapTuple	tuple;
	ScanKeyData scankey;
	SysScanDesc sscan;
	int			numDel = 0;

	/* first, delete the old master */
	ScanKeyInit(&scankey,
				Anum_gp_segment_configuration_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(master_dbid));
	sscan = systable_beginscan(rel, GpSegmentConfigDbidIndexId, true,
							   NULL, 1, &scankey);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		CatalogTupleDelete(rel, &tuple->t_self);
		numDel++;
	}
	systable_endscan(sscan);

	if (0 == numDel)
		elog(ERROR, "cannot find old master, dbid %i", master_dbid);

	/* now, set out rows for old standby. */
	ScanKeyInit(&scankey,
				Anum_gp_segment_configuration_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(standby_dbid));
	sscan = systable_beginscan(rel, GpSegmentConfigDbidIndexId, true,
							   NULL, 1, &scankey);

	tuple = systable_getnext(sscan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cannot find standby, dbid %i", standby_dbid);

	tuple = heap_copytuple(tuple);
	/* old standby keeps its previous dbid. */
	((Form_gp_segment_configuration) GETSTRUCT(tuple))->role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
	((Form_gp_segment_configuration) GETSTRUCT(tuple))->preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;

	CatalogTupleUpdate(rel, &tuple->t_self, tuple);

	systable_endscan(sscan);

	heap_close(rel, NoLock);
}

/*
 * Update gp_segment_configuration to activate a standby.
 */
static void
catalog_activate_standby(int16 standby_dbid, int16 master_dbid)
{
	segment_config_activate_standby(standby_dbid, master_dbid);
}

/*
 * Activate a standby. To do this, we need to update gp_segment_configuration.
 *
 * Returns:
 *  true upon success, otherwise throws error.
 */
bool
gp_activate_standby(void)
{
	int16		standby_dbid = GpIdentity.dbid;
	int16		master_dbid;

	master_dbid = contentid_get_dbid(MASTER_CONTENT_ID, GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, true);

	/*
	 * This call comes from Startup process post checking state in pg_control
	 * file to make sure its standby. If user calls (ideally SHOULD NOT) but
	 * just for troubleshooting or wired case must make usre its only executed
	 * on Standby. So, this checking should return after matching DBIDs only
	 * for StartUp Process, to cover for case of crash after updating the
	 * catalogs during promote.
	 */
	if (am_startup && (master_dbid == standby_dbid))
	{
		/*
		 * Job is already done, nothing needs to be done. We mostly crashed
		 * after updating the catalogs.
		 */
		return true;
	}

	mirroring_sanity_check(SUPERUSER | UTILITY_MODE | STANDBY_ONLY,
						   PG_FUNCNAME_MACRO);

	catalog_activate_standby(standby_dbid, master_dbid);

	/* done */
	return true;
}

Datum
gp_request_fts_probe_scan(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(ERROR,
				(errmsg("this function can only be called by master (without utility mode)")));
		PG_RETURN_BOOL(false);
	}

	FtsNotifyProber();

	PG_RETURN_BOOL(true);
}
