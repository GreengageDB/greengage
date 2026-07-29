# Copyright (c) 2021-2025, PostgreSQL Global Development Group

=pod

=head1 NAME

RecursiveCopy - compatibility shim for the historical C<RecursiveCopy> module name

=head1 SYNOPSIS

  use RecursiveCopy;

  RecursiveCopy::copypath($from, $to);

=head1 DESCRIPTION

Upstream PostgreSQL commit b3b4d8e68ae ("Move Perl test modules to a better
namespace") renamed C<RecursiveCopy> to C<PostgreSQL::Test::RecursiveCopy> and
deleted the old-name module.  GPDB deliberately keeps a number of old-API TAP
tests (and the old-API C<PostgresNode> module) that still say
C<use RecursiveCopy;> and call C<RecursiveCopy::copypath(...)>.

This module is a thin compatibility shim: it makes C<RecursiveCopy> an alias
for C<PostgreSQL::Test::RecursiveCopy> by replacing the whole
C<RecursiveCopy::> symbol table with C<PostgreSQL::Test::RecursiveCopy::>, so
C<RecursiveCopy::copypath> resolves to the real implementation.

=cut

package RecursiveCopy;

use strict;
use warnings;

use PostgreSQL::Test::RecursiveCopy ();

# Alias the entire RecursiveCopy:: stash onto PostgreSQL::Test::RecursiveCopy::.
# Do it at compile time (BEGIN) so that anything loaded afterwards already sees
# the alias in place.
BEGIN
{
	*RecursiveCopy:: = *PostgreSQL::Test::RecursiveCopy::;
}

1;
