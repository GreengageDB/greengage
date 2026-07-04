override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpos/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpopt/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libnaucrates/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpdbcost/include $(CPPFLAGS)
# Do not omit frame pointer. Even with RELEASE builds, it is used for
# backtracing.
# PG16 configure adds -Wshadow=compatible-local; vendored ORCA C++ has many
# pre-existing (harmless) shadows. Disable it for the gporca subtree only
# (must come after $(CXXFLAGS) so it wins).
override CXXFLAGS := -Werror -Wextra -Wpedantic -fno-omit-frame-pointer $(filter-out -Wshadow=compatible-local,$(CXXFLAGS))
# same for the LLVM bitcode (.bc) compile, which uses BITCODE_CXXFLAGS
override BITCODE_CXXFLAGS := $(filter-out -Wshadow=compatible-local,$(BITCODE_CXXFLAGS))
