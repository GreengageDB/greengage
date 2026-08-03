#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for libpq subsystem (backend half of libpq interface)
#
# IDENTIFICATION
#    src/backend/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/libpq
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS += -I$(libpq_srcdir) -I$(top_builddir)/src/port

# be-fsstubs is here for historical reasons, probably belongs elsewhere

OBJS = \
	auth-oauth.o \
	auth-sasl.o \
	auth-scram.o \
	auth.o \
	be-fsstubs.o \
	be-secure-common.o \
	be-secure.o \
	crypt.o \
	hba.o \
	ifaddr.o \
	pqcomm.o \
	pqformat.o \
	pqmq.o \
	pqsignal.o

ifeq ($(with_ssl),openssl)
OBJS += be-secure-openssl.o
endif

ifeq ($(with_gssapi),yes)
OBJS += be-gssapi-common.o be-secure-gssapi.o
endif

# Greengage objects follow
OBJS += fe-protocol3.o fe-connect.o fe-cancel.o \
        fe-exec.o libpq-events.o pqexpbuffer.o fe-auth.o fe-misc.o fe-secure.o fe-trace.o \
	fe-auth-scram.o \
	fe-auth-oauth.o \
        $(filter getpeereid.o, $(LIBOBJS))

# Greengage OpenSSL objects follow
ifeq ($(with_ssl),openssl)
OBJS += fe-secure-common.o fe-secure-openssl.o
endif

# Greengage GSSAPI objects follow
ifeq ($(with_gssapi),yes)
OBJS += fe-gssapi-common.o fe-secure-gssapi.o
endif

fe-protocol3.c fe-connect.c fe-cancel.c fe-exec.c libpq-events.c pqexpbuffer.c fe-auth.c fe-auth-scram.c fe-auth-oauth.c fe-misc.c fe-secure.c fe-secure-openssl.c fe-secure-common.c fe-secure-gssapi.c fe-gssapi-common.c fe-trace.c: % : $(top_srcdir)/src/interfaces/libpq/%
	rm -f $@ && $(LN_S) $< .

getpeereid.c: % : $(top_srcdir)/src/port/%
	rm -f $@ && $(LN_S) $< .

# Greengage: the backend's dispatch libpq never runs the client OAuth flow, so
# build fe-auth-oauth with the dynamic (dlopen/dlsym) path.  That avoids a
# link-time dependency on the curl-based libpq-oauth flow (pg_fe_run_oauth_flow),
# which is not built into the backend, while still providing pg_oauth_mech.
fe-auth-oauth.o: override CPPFLAGS += -DUSE_DYNAMIC_OAUTH

fe-connect.o: fe-connect.c $(top_builddir)/src/port/pg_config_paths.h

$(top_builddir)/src/port/pg_config_paths.h:
	$(MAKE) -C $(top_builddir)/src/port pg_config_paths.h

clean distclean: clean-symlinks

clean-symlinks:
	rm -f fe-protocol3.c fe-connect.c fe-cancel.c fe-exec.c libpq-events.c pqexpbuffer.c fe-auth.c fe-auth-scram.c fe-auth-oauth.c fe-misc.c fe-secure.c fe-secure-openssl.c fe-secure-common.c fe-secure-gssapi.c fe-gssapi-common.c fe-trace.c
	rm -f getpeereid.c

include $(top_srcdir)/src/backend/common.mk
