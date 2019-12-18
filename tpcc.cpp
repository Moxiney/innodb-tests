#include "tpcc.h"
#include "common.h"
#include <assert.h>

int done = 0;

ib_err_t ib_tbl_t::create_table(const char *dbname) {
    // To do
    return DB_SUCCESS;
}

ib_err_t ib_tbl_t::drop_table(const char *dbname) {
    // To do
    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init() {
    // To do: init db and tables
    auto err = database_init(dbname);
    ASSERT(err);
    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::shutdown() {
    // To do: drop tables and shutdown
    return DB_SUCCESS;
}

ib_err_t tpcc_run_txn(tpcc_db_t db) {
    // To do:
}
