#include "tpcc.h"
#include "common.h"
#include "tpcc-aux.h"
#include "tpcc-helper.h"
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
        for (auto idx_col : idx_cols)
        {
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

    for (auto tbl : tbls)
    {
        // LOG_INFO start to init table tbl.name
        // printf("start to init table %s.\n", tbl.name);
        err = tbl.create_table(dbname);
        ASSERT(err);
        printf("created table %s.\n", tbl.name);
    }

    // insert rows to tables
    err = init_tables_data();
    ASSERT(err);
    return err;
}

ib_err_t tpcc_db_t::init_tables_data()
{
    // To do: Huge work
    auto err = init_item_data();
    return err;
}

ib_err_t tpcc_db_t::shutdown()
{
    // To do: drop tables and shutdown
    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_item_data()
{
    // To do: create a transaction to insert tuple
    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;
    auto item_tbl = tbls[item];

    printf("Begin init_data transaction\n");
    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    printf("Open cursor of %s\n", item_tbl.name);
    err = open_table(dbname, tbls[item].name, ib_trx, &crsr);
    ASSERT(err);

    printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    ASSERT(err);

    auto tpl = ib_clust_read_tuple_create(crsr);

    for (ib_ulint_t i = 0; i < 1; i++) {
        // insert tuple with id = i
        // To do
        err = ib_col_set_value(tpl, I_ID, &i, item_tbl.cols[I_ID].len);
        ASSERT(err);
        printf("rand %d\n", Rand(0,5));



        err = ib_cursor_insert_row(crsr, tpl);
        ASSERT(err);
        tpl = ib_tuple_clear(tpl);
        ASSERT(err);
    }

    // printf("Insert rows\n");
    // err = insert_rows(crsr);
    // ASSERT(err);

    printf("Query table\n");
    err = do_query(crsr);
    ASSERT(err);;

    printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    ASSERT(err);
    crsr = NULL;

    printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);


    return DB_SUCCESS;
}
ib_err_t tpcc_db_t::init_wh_data()
{
    // To do
    return DB_SUCCESS;
}
ib_err_t tpcc_db_t::init_dist_data(){
    // To do
    return DB_SUCCESS;
}
ib_err_t tpcc_db_t::init_cust_data(){
    // To do
    return DB_SUCCESS;
}
ib_err_t tpcc_db_t::init_stock_data(){
    // To do
    return DB_SUCCESS;
}
ib_err_t tpcc_db_t::init_hist_data(){
    // To do
    return DB_SUCCESS;
}
ib_err_t tpcc_db_t::init_order_data(){
    return DB_SUCCESS;
}

