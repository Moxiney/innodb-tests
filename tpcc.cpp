#include "tpcc.h"

int done = 0;

ib_err_t ib_tbl_t::create_table(const char *dbname) {
    // To do
    return DB_SUCCESS;
}

ib_err_t ib_tbl_t::drop_table(const char *dbname) {
    // To do
    return DB_SUCCESS;
}

ib_err_t ib_db_t::init() {
    // To do: init db and tables
    return DB_SUCCESS;
}

ib_err_t ib_db_t::shutdown() {
    // To do: drop tables and shutdown
    return DB_SUCCESS;
}

ib_err_t tpcc_run_txn(ib_db_t db) {
    // To do:
}
