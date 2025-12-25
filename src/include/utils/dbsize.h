/*-------------------------------------------------------------------------
 *
 * dbsize.h
 *	  Functions for relation disk space estimation.
 *
 * IDENTIFICATION
 *	  src/include/utils/dbsize.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBSIZE_H
#define DBSIZE_H

#include "utils/relcache.h"
#include "common/relpath.h"

/*
 * calculate size of (one fork of) a relation. Details in dbsize.c
 */
int64 calculate_relation_size(Relation rel, ForkNumber forknum,
							  bool include_ao_aux, bool ao_physical_size,
							  int stat_error_level);

#endif    /* DBSIZE_H */
