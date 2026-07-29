# Copyright (c) 2021-2025, PostgreSQL Global Development Group

=pod

=head1 NAME

TestLib - compatibility shim for the historical C<TestLib> module name

=head1 SYNOPSIS

  use TestLib;

=head1 DESCRIPTION

Upstream PostgreSQL commit b3b4d8e68ae ("Move Perl test modules to a better
namespace") renamed C<TestLib> to C<PostgreSQL::Test::Utils> and deleted the
old-name module.  GPDB deliberately keeps a number of old-API TAP tests (and
the old-API C<PostgresNode> module) that still say C<use TestLib;> and refer to
symbols such as C<$TestLib::windows_os>, C<$TestLib::tmp_check>,
C<$TestLib::log_path> and C<TestLib::system_or_bail(...)>.

This module is a thin compatibility shim: it makes C<TestLib> an alias for
C<PostgreSQL::Test::Utils>.  By replacing the whole C<TestLib::> symbol table
with C<PostgreSQL::Test::Utils::> we carry over every function, every
C<@EXPORT>/C<@ISA> entry and the Exporter-provided C<import()>, plus every
package variable (C<$windows_os>, C<$is_msys2>, C<$use_unix_sockets>,
C<$timeout_default>, C<$tmp_check>, C<$log_path>, C<$test_logfile>, ...).  As a
result C<use TestLib;> imports exactly what C<use PostgreSQL::Test::Utils;>
would, and any fully-qualified C<TestLib::foo> / C<$TestLib::foo> reference
resolves to the corresponding C<PostgreSQL::Test::Utils> symbol.

=cut

package TestLib;

use strict;
use warnings;

use PostgreSQL::Test::Utils ();

# Alias the entire TestLib:: stash onto PostgreSQL::Test::Utils::.  This must
# happen at compile time (before any "use TestLib" import runs), so do it in a
# BEGIN block that follows the "use" above.  After this, TestLib:: and
# PostgreSQL::Test::Utils:: are the same symbol table, so @TestLib::EXPORT,
# @TestLib::ISA and TestLib::import() are all the real Utils ones.
BEGIN
{
	*TestLib:: = *PostgreSQL::Test::Utils::;
}

1;
