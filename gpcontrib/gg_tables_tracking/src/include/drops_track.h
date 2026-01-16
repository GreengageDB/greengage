#ifndef DROPS_TRACK_H
#define DROPS_TRACK_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "storage/relfilenode.h"

void		drops_track_init(void);
void		drops_track_deinit(void);

void		drops_track_add(RelFileNode relNode);
List	   *drops_track_move(Oid dbid);

#endif   /* DROPS_TRACK_H */
