#pragma once
#include "/usr/local/include/embedded_innodb-1.0/innodb.h"


extern int init_table_size;

// create a table and insert values.
ib_err_t ycsb_init(
    const char*	dbname,			/*!< in: database name */
	const char*	name);

ib_err_t ycsb_run_txn(
    const char *dbname,
    const char *name,
    int read_ratio);

