#ifndef DBSIZE_H
#define DBSIZE_H

#include "catalog/pg_class.h"

int64		dbsize_calc_size(Form_pg_class relInfo);

#endif   /* DBSIZE_H */
