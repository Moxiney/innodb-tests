#include "tpcc.h"
#include "common.h"
#include <assert.h>

int done = 0;

ib_err_t ib_tbl_t::create_table(const char *dbname)
{
    // To do
    ib_trx_t ib_trx;
    ib_id_t table_id = 0;
    ib_err_t err = DB_SUCCESS;
    ib_tbl_sch_t ib_tbl_sch = NULL;
    ib_idx_sch_t ib_idx_sch = NULL;
    char table_name[IB_MAX_TABLE_NAME_LEN];

    snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
    // printf("Creating %s\n", table_name);

    /* Pass a table page size of 0, ie., use default page size. */
    err = ib_table_schema_create(
        table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);
    ASSERT(err);

    /* Add cols*/
    for (auto col : cols)
    {
        err = ib_table_schema_add_col(
            ib_tbl_sch,
            col.name,
            col.type,
            IB_COL_NONE,
            0,
            col.len);
        // printf("Add column %s\n", col.name);

        ASSERT(err);
    }

    // printf("Creating %s: index %s\n", table_name, idx_name);
    if (idx_cols.size() > 0)
    {
        // If index is not empty.
        err = ib_table_schema_add_index(ib_tbl_sch, idx_name, &ib_idx_sch);
        ASSERT(err);

        /* Add cols to index schema and Set prefix length to 0. */
        for (auto idx_col: idx_cols) {
            err = ib_index_schema_add_col(ib_idx_sch, idx_col.name, 0);
            ASSERT(err);
            // printf("Add index column %s\n", idx_col.name);
        }

        err = ib_index_schema_set_clustered(ib_idx_sch);
        ASSERT(err);
    }

    /* create table */
    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    err = ib_schema_lock_exclusive(ib_trx);
    ASSERT(err);

    err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
    ASSERT(err);

    err = ib_trx_commit(ib_trx);
    ASSERT(err);

    if (ib_tbl_sch != NULL)
    {
        ib_table_schema_delete(ib_tbl_sch);
    }

    return err;
}

ib_err_t ib_tbl_t::drop_table(const char *dbname)
{
    // To do
    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init()
{
    // To do: init db and tables
    auto err = database_init(dbname);
    ASSERT(err);

    for (auto tbl : tbls) {
        // LOG_INFO start to init table tbl.name
        // printf("start to init table %s.\n", tbl.name);
        err = tbl.create_table(dbname);
        ASSERT(err);
        printf("created table %s.\n", tbl.name);
    }

    // insert rows to tables
    err = insert_rows();
    ASSERT(err);
    return err;
}

ib_err_t tpcc_db_t::insert_rows() {
    // To do: Huge work
}


ib_err_t tpcc_db_t::shutdown()
{
    // To do: drop tables and shutdown
    return DB_SUCCESS;
}

ib_err_t tpcc_run_txn(tpcc_db_t db)
{
    // To do:
}
