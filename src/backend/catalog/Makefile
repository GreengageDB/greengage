#-------------------------------------------------------------------------
#
# Makefile for backend/catalog
#
# Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/catalog/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/catalog
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	aclchk.o \
	catalog.o \
	dependency.o \
	heap.o \
	index.o \
	indexing.o \
	namespace.o \
	objectaccess.o \
	objectaddress.o \
	partition.o \
	pg_aggregate.o \
	pg_attrdef.o \
	pg_cast.o \
	pg_class.o \
	pg_collation.o \
	pg_constraint.o \
	pg_conversion.o \
	pg_db_role_setting.o \
	pg_depend.o \
	pg_enum.o \
	pg_inherits.o \
	pg_largeobject.o \
	pg_namespace.o \
	pg_operator.o \
	pg_parameter_acl.o \
	pg_proc.o \
	pg_publication.o \
	pg_range.o \
	pg_shdepend.o \
	pg_subscription.o \
	pg_type.o \
	storage.o \
	toasting.o

OBJS += pg_extprotocol.o \
       pg_proc_callback.o \
       aoseg.o aoblkdir.o gp_fastsequence.o gp_segment_config.o \
       pg_attribute_encoding.o pg_compression.o aovisimap.o \
       pg_appendonly.o \
       oid_dispatch.o aocatalog.o storage_tablespace.o storage_database.o \
       storage_tablespace_twophase.o storage_tablespace_xact.o \
       gp_partition_template.o


CATALOG_JSON:= $(addprefix $(top_srcdir)/gpMgmt/bin/gppylib/data/, $(addsuffix .json,$(GP_MAJORVERSION)))

include $(top_srcdir)/src/backend/common.mk

# GPDB: the genbki-driving header/data lists and the bki-stamp/header-stamp
# machinery moved to src/include/catalog/Makefile in PG17 (upstream 6ab2e8385d5).
# We keep the GPDB catalog header list here ONLY to feed the GPDB-specific
# catalog_json generation target below (invoked by
# gpMgmt/bin/gppylib/data/Makefile); the GPDB catalog headers themselves must
# also be registered in src/include/catalog/Makefile so genbki emits them.
CATALOG_HEADERS := \
	pg_proc.h pg_type.h pg_attribute.h pg_class.h \
	pg_attrdef.h pg_constraint.h pg_inherits.h pg_index.h pg_operator.h \
	pg_opfamily.h pg_opclass.h pg_am.h pg_amop.h pg_amproc.h \
	pg_language.h pg_largeobject_metadata.h pg_largeobject.h pg_aggregate.h \
	pg_statistic.h pg_statistic_ext.h pg_statistic_ext_data.h \
	pg_rewrite.h pg_trigger.h pg_event_trigger.h pg_description.h \
	pg_cast.h pg_enum.h pg_namespace.h pg_conversion.h pg_depend.h \
	pg_database.h pg_db_role_setting.h pg_tablespace.h \
	pg_authid.h pg_auth_members.h pg_shdepend.h pg_shdescription.h \
	pg_ts_config.h pg_ts_config_map.h pg_ts_dict.h \
	pg_ts_parser.h pg_ts_template.h pg_extension.h \
	pg_foreign_data_wrapper.h pg_foreign_server.h pg_user_mapping.h \
	pg_resqueue.h pg_resqueuecapability.h pg_resourcetype.h \
	pg_resgroup.h pg_resgroupcapability.h \
	gp_configuration_history.h gp_id.h gp_distribution_policy.h gp_version_at_initdb.h \
	gp_segment_configuration.h \
	pg_appendonly.h \
	gp_fastsequence.h pg_extprotocol.h \
	pg_attribute_encoding.h \
	pg_auth_time_constraint.h \
	pg_compression.h \
	pg_proc_callback.h \
	pg_type_encoding.h \
	pg_stat_last_operation.h pg_stat_last_shoperation.h \
	pg_foreign_table.h pg_policy.h pg_replication_origin.h \
	pg_default_acl.h pg_init_privs.h pg_seclabel.h pg_shseclabel.h \
	pg_collation.h pg_parameter_acl.h pg_partitioned_table.h \
	pg_range.h pg_transform.h \
	pg_sequence.h pg_publication.h pg_publication_namespace.h \
	pg_publication_rel.h pg_subscription.h pg_subscription_rel.h \
	gp_partition_template.h

# In the list of headers, indexing.h needs to be last, and toasting.h just
# before it.  This ensures we don't try to create indexes or toast tables
# before their catalogs exist.
POSTGRES_BKI_SRCS := $(addprefix $(top_srcdir)/src/include/catalog/,\
	$(CATALOG_HEADERS) toasting.h indexing.h \
	)

catalog_json: $(CATALOG_JSON)

$(CATALOG_JSON): process_foreign_keys.pl $(POSTGRES_BKI_SRCS) $(top_srcdir)/src/include/catalog/catversion.h
	cat $(POSTGRES_BKI_SRCS) $(top_srcdir)/src/include/catalog/catversion.h | $(PERL) process_foreign_keys.pl > $@

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(srcdir)/system_functions.sql '$(DESTDIR)$(datadir)/system_functions.sql'
	$(INSTALL_DATA) $(srcdir)/system_views.sql '$(DESTDIR)$(datadir)/system_views.sql'
	$(INSTALL_DATA) $(srcdir)/information_schema.sql '$(DESTDIR)$(datadir)/information_schema.sql'
	$(INSTALL_DATA) $(call vpathsearch,cdb_schema.sql) '$(DESTDIR)$(datadir)/cdb_init.d/cdb_schema.sql'
	$(INSTALL_DATA) $(srcdir)/sql_features.txt '$(DESTDIR)$(datadir)/sql_features.txt'
	$(INSTALL_DATA) $(call vpathsearch,gp_toolkit.sql) '$(DESTDIR)$(datadir)/cdb_init.d/gp_toolkit.sql'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/cdb_init.d'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, system_functions.sql system_views.sql information_schema.sql cdb_init.d/cdb_schema.sql sql_features.txt)
