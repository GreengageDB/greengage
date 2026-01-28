#ifndef TRACK_DBSIZE_H
#define TRACK_DBSIZE_H

#include "catalog/pg_class.h"

int64		dbsize_calc_size(Form_pg_class relInfo);

#endif   /* TRACK_DBSIZE_H */
