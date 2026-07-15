/*
 * This is a mock version of src/backend/main/main.c. In a unit test, the test
 * program contains the real main() function, so we don't want to link the
 * postgres backend's main() function into the test program. (Alternatively,
 * we could use ld's --wrap option and call the test program's main()
 * __wrap_main(), but this seems nicer.)
 */
#include "postgres.h"

#include "postmaster/postmaster.h"

/* The only thing we need from main.c is this global variable */
const char *progname;

/*
 * GPDB: PG18 factored dispatch-option parsing out of main() into
 * parse_dispatch_option().  It still lives in main.c, but is now also called
 * from bootstrap.c, postmaster.c and postgres.c -- all of which ARE linked
 * into the mock test programs.  Since we omit main.o, stub it here so the link
 * resolves.  Test programs never dispatch to a subprogram, so returning
 * DISPATCH_POSTMASTER (the real function's "no match" default) is sufficient.
 */
DispatchOption
parse_dispatch_option(const char *name)
{
	return DISPATCH_POSTMASTER;
}
