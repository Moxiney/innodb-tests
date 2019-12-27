#include "tpcc.h"
#include "common.h"
#include "tpcc-aux.h"
#include "tpcc-helper.h"
#include <assert.h>
#include <thread>

int done = 0;
int num_wh = 1;

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
    //for (auto idx : idxs)
    for (int i = 0; i < idxs.size(); i++)
    {
        auto idx = idxs[i];
        err = ib_table_schema_add_index(ib_tbl_sch, idx.name, &ib_idx_sch);
        ASSERT(err);

        /* Add cols to index schema and Set prefix length to 0. */
        for (auto idx_col : idx.cols)
        {
            err = ib_index_schema_add_col(ib_idx_sch, idx_col.name, 0);
            ASSERT(err);
            printf("Add index col  %s\n", idx_col.name);
        }

        if (i == 0)
        {
            err = ib_index_schema_set_clustered(ib_idx_sch);
            ASSERT(err);
        }
        printf("Add index  %s\n", idx.name);
    }

    // if (idx_cols.size() > 0)
    // {
    //     // If index is not empty.
    //     err = ib_table_schema_add_index(ib_tbl_sch, idx_name, &ib_idx_sch);
    //     ASSERT(err);

    //     /* Add cols to index schema and Set prefix length to 0. */
    //     for (auto idx_col : idx_cols)
    //     {
    //         err = ib_index_schema_add_col(ib_idx_sch, idx_col.name, 0);
    //         ASSERT(err);
    //         // printf("Add index column %s\n", idx_col.name);
    //     }

    //     err = ib_index_schema_set_clustered(ib_idx_sch);
    //     ASSERT(err);
    // }

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

    tpcc_rdm.resize(num_wh);

    auto err = init_item_data();
    // for (ib_ulint_t wh_id = 1; wh_id <= num_wh; wh_id++)
    // {
    //     init_wh_data(wh_id);
    //     init_dist_data(wh_id);
    //     init_stock_data(wh_id);
    //     for (ib_ulint_t dist_id = 1; dist_id <= TPCCConfig::g_dist_per_ware; dist_id++)
    //     {
    //         init_cust_data(wh_id, dist_id);
    //         init_order_data(wh_id, dist_id);
    //         for (uint64_t cust_id = 1; cust_id <= TPCCConfig::g_cust_per_dist; cust_id++)
    //         {
    //             //wl->init_tab_hist(cid, did, wid);
    //             init_hist_data(wh_id, dist_id, cust_id);
    //         }
    //     }
    // }

    std::thread threads[num_wh];
    for (ib_ulint_t wh_id = 1; wh_id <= num_wh; wh_id++)
    {
        threads[wh_id-1] = std::thread(
            [&](ib_ulint_t wh_id) {
                printf("thread %ld start to init data of wh %ld\n", wh_id, wh_id);
                init_wh_data(wh_id);
                init_dist_data(wh_id);
                init_stock_data(wh_id);
                for (ib_ulint_t dist_id = 1; dist_id <= TPCCConfig::g_dist_per_ware; dist_id++)
                {
                    init_cust_data(wh_id, dist_id);
                    init_order_data(wh_id, dist_id);
                    for (uint64_t cust_id = 1; cust_id <= TPCCConfig::g_cust_per_dist; cust_id++)
                    {
                        //wl->init_tab_hist(cid, did, wid);
                        init_hist_data(wh_id, dist_id, cust_id);
                    }
                }
                
            },
            wh_id);
    }
    for (ib_ulint_t wh_id = 1; wh_id <= num_wh; wh_id++)
    {
        threads[wh_id-1].join();
    }
    printf("data initialized \n");
    // exit(0);
    return err;
}

ib_err_t tpcc_db_t::shutdown()
{
    // To do: drop tables and shutdown
    ib_err_t err;

    err = ib_shutdown(IB_SHUTDOWN_NORMAL);
    assert(err == DB_SUCCESS);
    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_item_data()
{
    // To do: create a transaction to insert tuple
    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;
    auto item_tbl = tbls[item];

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    err = open_table(dbname, item_tbl.name, ib_trx, &crsr);
    ASSERT(err);

    //printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    ASSERT(err);

    auto tpl = ib_clust_read_tuple_create(crsr);

    for (ib_ulint_t i = 1; i <= TPCCConfig::g_max_items; i++)
    {
        // insert tuple with id = i
        // To do
        err = ib_col_set_value(tpl, I_ID, &i, item_tbl.cols[I_ID].len);
        ASSERT(err);

        auto val = URand(1, 10000, 0);
        err = ib_col_set_value(tpl, I_IM_ID, &val, item_tbl.cols[I_IM_ID].len);
        ASSERT(err);

        auto name = MakeAlphaString(14, 24, 0);
        err = ib_col_set_value(tpl, I_NAME, name.c_str(), name.size());
        ASSERT(err);

        val = URand(1, 100, 0);
        err = ib_col_set_value(tpl, I_PRICE, &val, item_tbl.cols[I_PRICE].len);
        ASSERT(err);

        auto data = MakeAlphaString(26, 50, 0);
        if (Rand(10, 0) == 0)
        {
            data = "original";
        }
        err = ib_col_set_value(tpl, I_DATA, data.c_str(), data.size());

        err = ib_cursor_insert_row(crsr, tpl);
        ASSERT(err);
        tpl = ib_tuple_clear(tpl);
        ASSERT(err);
    }

    printf("Item table initialization completed\n");
    err = do_query(crsr);
    ASSERT(err);
    ;

    // printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    ASSERT(err);
    crsr = NULL;

    // printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);

    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_wh_data(ib_ulint_t wh_id)
{
    // To do

    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;
    auto wh_tbl = tbls[warehouse];

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    //printf("Open cursor of %s\n", wh_tbl.name);
    err = open_table(dbname, wh_tbl.name, ib_trx, &crsr);
    ASSERT(err);

    //printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    ASSERT(err);

    auto tpl = ib_clust_read_tuple_create(crsr);

    // modified tuple and insert
    std::string field;

    err = ib_col_set_value(tpl, I_ID, &wh_id, wh_tbl.cols[I_ID].len);
    ASSERT(err);

    field = MakeAlphaString(6, 10, wh_id - 1);
    err = ib_col_set_value(tpl, W_NAME, field.c_str(), field.size());
    ASSERT(err);

    field = MakeAlphaString(10, 20, wh_id - 1);
    err = ib_col_set_value(tpl, W_STREET_1, field.c_str(), field.size());
    ASSERT(err);

    field = MakeAlphaString(10, 20, wh_id - 1);
    err = ib_col_set_value(tpl, W_STREET_2, field.c_str(), field.size());
    ASSERT(err);

    field = MakeAlphaString(10, 20, wh_id - 1);
    err = ib_col_set_value(tpl, W_CITY, field.c_str(), field.size());
    ASSERT(err);

    field = MakeAlphaString(2, 2, wh_id - 1);
    err = ib_col_set_value(tpl, W_STATE, field.c_str(), field.size());
    ASSERT(err);

    field = MakeNumberString(9, 9, wh_id - 1);
    err = ib_col_set_value(tpl, W_ZIP, field.c_str(), field.size());
    ASSERT(err);

    double tax = (double)URand(0L, 200L, wh_id - 1) / 1000.0;
    err = ib_col_set_value(tpl, W_TAX, &tax, wh_tbl.cols[W_TAX].len);
    ASSERT(err);

    double w_ytd = 300000.00;
    err = ib_col_set_value(tpl, W_YTD, &w_ytd, wh_tbl.cols[W_YTD].len);
    ASSERT(err);

    err = ib_cursor_insert_row(crsr, tpl);
    ASSERT(err);
    tpl = ib_tuple_clear(tpl);
    ASSERT(err);

    // Print all tuples
    // if (wh_id == num_wh)
    // {
    //     printf("Warehouse table initialization completed.\n");
    //     err = do_query(crsr);
    //     ASSERT(err);
    // }

    // printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    ASSERT(err);
    crsr = NULL;

    // printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);

    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_dist_data(ib_ulint_t wh_id)
{
    // To do

    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;
    auto dist_tbl = tbls[district];

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    //printf("Open cursor of %s\n", dist_tbl.name);
    err = open_table(dbname, dist_tbl.name, ib_trx, &crsr);
    ASSERT(err);

    //printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    ASSERT(err);

    auto tpl = ib_clust_read_tuple_create(crsr);

    // modified tuple and insert

    for (ib_ulint_t dist_id = 1; dist_id <= TPCCConfig::g_dist_per_ware; dist_id++)
    {
        std::string field;
        ASSERT(ib_col_set_value(tpl, D_ID, &dist_id, dist_tbl.cols[D_ID].len));
        ASSERT(ib_col_set_value(tpl, D_W_ID, &wh_id, dist_tbl.cols[D_W_ID].len));
        field = MakeAlphaString(6, 10, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, D_NAME, field.c_str(), field.size()));
        field = MakeAlphaString(10, 20, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, D_STREET_1, field.c_str(), field.size()));
        field = MakeAlphaString(10, 20, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, D_STREET_2, field.c_str(), field.size()));
        field = MakeAlphaString(10, 20, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, D_CITY, field.c_str(), field.size()));
        field = MakeAlphaString(2, 2, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, D_STATE, field.c_str(), field.size()));
        field = MakeAlphaString(9, 9, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, D_ZIP, field.c_str(), field.size()));
        double tax = (double)URand(0L, 200L, wh_id - 1) / 1000.0;
        double w_ytd = 30000.00;
        ib_ulint_t next_oid = 3001;
        ASSERT(ib_col_set_value(tpl, D_TAX, &tax, dist_tbl.cols[D_TAX].len));
        // t_district->set_row_value_double(data, D_TAX, tax);
        ASSERT(ib_col_set_value(tpl, D_YTD, &w_ytd, dist_tbl.cols[D_YTD].len));
        // t_district->set_row_value_double(data, D_YTD, w_ytd);
        ASSERT(ib_col_set_value(tpl, D_NEXT_O_ID, &next_oid, dist_tbl.cols[D_NEXT_O_ID].len));
        // t_district->set_row_value_int(data, D_NEXT_O_ID, 3001);

        err = ib_cursor_insert_row(crsr, tpl);
        ASSERT(err);
        tpl = ib_tuple_clear(tpl);
        ASSERT(err);
    }
    // if (wh_id == num_wh)
    // {
    //     printf("District table initialization committed.\n");
    //     err = do_query(crsr);
    //     ASSERT(err);
    // }

    // printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    ASSERT(err);
    crsr = NULL;

    // printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);

    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_stock_data(ib_ulint_t wh_id)
{
    // To do

    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;
    auto stock_tbl = tbls[stock];

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    //printf("Open cursor of %s\n", stock_tbl.name);
    err = open_table(dbname, stock_tbl.name, ib_trx, &crsr);
    ASSERT(err);

    //printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    ASSERT(err);

    auto tpl = ib_clust_read_tuple_create(crsr);

    // modified tuple and insert

    for (ib_ulint_t s_id = 1; s_id <= TPCCConfig::g_max_items; s_id++)
    {
        std::string field;
        ASSERT(ib_col_set_value(tpl, S_I_ID, &s_id, stock_tbl.cols[S_I_ID].len));
        ASSERT(ib_col_set_value(tpl, S_W_ID, &wh_id, stock_tbl.cols[S_W_ID].len));
        auto val = URand(10, 100, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, S_QUANTITY, &val, stock_tbl.cols[S_QUANTITY].len));
        const ib_ulint_t cnt = 0;
        ASSERT(ib_col_set_value(tpl, S_REMOTE_CNT, &cnt, stock_tbl.cols[S_REMOTE_CNT].len));

        err = ib_cursor_insert_row(crsr, tpl);
        ASSERT(err);
        tpl = ib_tuple_clear(tpl);
        ASSERT(err);
    }
    // if (wh_id == num_wh)
    // {
    //     printf("Stock table initialization completed.\n");
    //     err = do_query(crsr);
    //     ASSERT(err);
    // }

    // printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    ASSERT(err);
    crsr = NULL;

    // printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);

    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_cust_data(ib_ulint_t wh_id, ib_ulint_t dist_id)
{
    // To do
    //printf("Customer table %ld-%ld initialization began.\n", wh_id, dist_id);
    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;
    auto cust_tbl = tbls[customer];

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    //printf("Open cursor of %s\n", cust_tbl.name);
    err = open_table(dbname, cust_tbl.name, ib_trx, &crsr);
    ASSERT(err);

    //printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    ASSERT(err);

    auto tpl = ib_clust_read_tuple_create(crsr);

    // modified tuple and insert

    for (ib_ulint_t cust_id = 1; cust_id <= TPCCConfig::g_cust_per_dist; cust_id++)
    {
        std::string field;
        std::string c_last;

        ASSERT(ib_col_set_value(tpl, C_ID, &cust_id, cust_tbl.cols[C_ID].len));
        ASSERT(ib_col_set_value(tpl, C_D_ID, &dist_id, cust_tbl.cols[C_D_ID].len));
        ASSERT(ib_col_set_value(tpl, C_W_ID, &wh_id, cust_tbl.cols[C_W_ID].len));

        field = "OE";
        ASSERT(ib_col_set_value(tpl, C_MIDDLE, field.c_str(), field.size()));
        c_last = cust_id < 1000 ? Lastname(cust_id - 1)
                                : Lastname(NURand(255, 0, 999, wh_id - 1));
        ASSERT(ib_col_set_value(tpl, C_LAST, c_last.c_str(), c_last.size()));

        field = MakeAlphaString(2, 2, wh_id - 1);
        ASSERT(ib_col_set_value(tpl, C_STATE, field.c_str(), field.size()));

        field = Rand(10, wh_id - 1) == 0 ? "GC" : "BC";
        ASSERT(ib_col_set_value(tpl, C_CREDIT, field.c_str(), field.size()));

        double discnt = (double)Rand(5000, wh_id - 1) / 10000;
        ASSERT(ib_col_set_value(tpl, C_DISCOUNT, &discnt, cust_tbl.cols[C_DISCOUNT].len));

        double balance = -10.0;
        ASSERT(ib_col_set_value(tpl, C_BALANCE, &balance, cust_tbl.cols[C_BALANCE].len));
        // t_customer->set_row_value_double(data, C_BALANCE, -10.0);

        double ytd_payment = 10.0;
        ASSERT(ib_col_set_value(tpl, C_YTD_PAYMENT, &ytd_payment, cust_tbl.cols[C_YTD_PAYMENT].len));
        // t_customer->set_row_value_double(data, C_YTD_PAYMENT, 10.0);

        ib_ulint_t payment_cnt = 1;
        ASSERT(ib_col_set_value(tpl, C_PAYMENT_CNT, &payment_cnt, cust_tbl.cols[C_PAYMENT_CNT].len));
        // t_customer->set_row_value_int(data, C_PAYMENT_CNT, 1);

        err = ib_cursor_insert_row(crsr, tpl);
        ASSERT(err);
        tpl = ib_tuple_clear(tpl);
        ASSERT(err);
    }
    // if (wh_id == num_wh && dist_id == TPCCConfig::g_dist_per_ware)
    // {
    //     printf("Customer table:\n");
    //     err = do_query(crsr);
    //     ASSERT(err);
    // }

    // printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    ASSERT(err);
    crsr = NULL;

    // printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);
    //printf("Customer table %ld-%ld initialization committed.\n\n", wh_id, dist_id);
    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_order_data(ib_ulint_t wh_id, ib_ulint_t dist_id)
{
    //printf("Order table %ld-%ld initialization began.\n", wh_id, dist_id);
    ib_err_t err;
    ib_crsr_t order_crsr, ol_crsr, no_crsr;
    ib_trx_t ib_trx;
    auto order_tbl = tbls[order];
    auto ol_tbl = tbls[order_line];
    auto no_tbl = tbls[new_order];

    ib_ulint_t perm[TPCCConfig::g_cust_per_dist];
    init_permutation(perm, wh_id);

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    //printf("Open cursor of %s\n", order_tbl.name);
    ASSERT(open_table(dbname, order_tbl.name, ib_trx, &order_crsr));

    //printf("Open cursor of %s\n", ol_tbl.name);
    ASSERT(open_table(dbname, ol_tbl.name, ib_trx, &ol_crsr));

    //printf("Open cursor of %s\n", no_tbl.name);
    ASSERT(open_table(dbname, no_tbl.name, ib_trx, &no_crsr));

    //printf("Lock table in IX\n");
    ASSERT(ib_cursor_lock(order_crsr, IB_LOCK_IX));
    ASSERT(ib_cursor_lock(ol_crsr, IB_LOCK_IX));
    ASSERT(ib_cursor_lock(no_crsr, IB_LOCK_IX));

    auto order_tpl = ib_clust_read_tuple_create(order_crsr);
    auto ol_tpl = ib_clust_read_tuple_create(ol_crsr);
    auto no_tpl = ib_clust_read_tuple_create(no_crsr);

    // modified tuple and insert

    for (ib_ulint_t o_id = 1; o_id <= TPCCConfig::g_cust_per_dist; o_id++)
    {
        std::string field;
        ib_ulint_t cust_id = perm[o_id - 1];

        // Insert order table

        ASSERT(ib_col_set_value(order_tpl, O_ID, &o_id, order_tbl.cols[O_ID].len));
        ASSERT(ib_col_set_value(order_tpl, O_C_ID, &cust_id, order_tbl.cols[O_C_ID].len));
        ASSERT(ib_col_set_value(order_tpl, O_D_ID, &dist_id, order_tbl.cols[O_D_ID].len));
        ASSERT(ib_col_set_value(order_tpl, O_W_ID, &wh_id, order_tbl.cols[O_D_ID].len));

        ib_ulint_t o_entry = 2019;
        ASSERT(ib_col_set_value(order_tpl, O_ENTRY_D, &o_entry, order_tbl.cols[O_ENTRY_D].len));
        ib_ulint_t carrier_id = o_id < 2101 ? URand(1, 10, wh_id - 1) : 0;
        ASSERT(ib_col_set_value(order_tpl, O_CARRIER_ID, &carrier_id, order_tbl.cols[O_CARRIER_ID].len));
        ib_ulint_t o_ol_cnt = URand(5, 15, wh_id - 1);
        ASSERT(ib_col_set_value(order_tpl, O_OL_CNT, &o_ol_cnt, order_tbl.cols[O_OL_CNT].len));
        ib_ulint_t val = 1;
        ASSERT(ib_col_set_value(order_tpl, O_ALL_LOCAL, &val, order_tbl.cols[O_ALL_LOCAL].len));

        err = ib_cursor_insert_row(order_crsr, order_tpl);
        ASSERT(err);
        order_tpl = ib_tuple_clear(order_tpl);
        ASSERT(err);

        // Insert order_line table
        for (ib_ulint_t ol = 1; ol <= o_ol_cnt; ol++)
        {
            ASSERT(ib_col_set_value(ol_tpl, OL_O_ID, &o_id, ol_tbl.cols[OL_O_ID].len));
            ASSERT(ib_col_set_value(ol_tpl, OL_D_ID, &dist_id, ol_tbl.cols[OL_D_ID].len));
            ASSERT(ib_col_set_value(ol_tpl, OL_W_ID, &wh_id, ol_tbl.cols[OL_W_ID].len));
            ASSERT(ib_col_set_value(ol_tpl, OL_NUMBER, &ol, ol_tbl.cols[OL_NUMBER].len));
            ib_ulint_t i_id = URand(1, TPCCConfig::g_max_items, wh_id - 1);
            ASSERT(ib_col_set_value(ol_tpl, OL_I_ID, &i_id, ol_tbl.cols[OL_I_ID].len));
            // t_orderline->set_row_value_int(ol_data, OL_O_ID, oid);
            // t_orderline->set_row_value_int(ol_data, OL_D_ID, d_id);
            // t_orderline->set_row_value_int(ol_data, OL_W_ID, w_id);
            // t_orderline->set_row_value_int(ol_data, OL_NUMBER, ol);
            // t_orderline->set_row_value_int(ol_data, OL_I_ID,
            //                                URand(1, 100000, w_id - 1));
            err = ib_cursor_insert_row(ol_crsr, ol_tpl);
            ASSERT(err);
            ol_tpl = ib_tuple_clear(ol_tpl);
            ASSERT(err);
        }

        // Insert new_order table
        if (o_id >= 0.7 * TPCCConfig::g_cust_per_dist)
        {
            ASSERT(ib_col_set_value(no_tpl, NO_O_ID, &o_id, ol_tbl.cols[NO_O_ID].len));
            ASSERT(ib_col_set_value(no_tpl, NO_D_ID, &dist_id, ol_tbl.cols[NO_D_ID].len));
            ASSERT(ib_col_set_value(no_tpl, NO_W_ID, &wh_id, ol_tbl.cols[NO_W_ID].len));
            // t_neworder->set_row_value_int(no_data, NO_O_ID, oid);
            // t_neworder->set_row_value_int(no_data, NO_D_ID, d_id);
            // t_neworder->set_row_value_int(no_data, NO_W_ID, w_id);

            err = ib_cursor_insert_row(no_crsr, no_tpl);
            ASSERT(err);
            no_tpl = ib_tuple_clear(no_tpl);
            ASSERT(err);
        }
    }

    // if (wh_id == num_wh && dist_id == TPCCConfig::g_dist_per_ware)
    // {
    //     printf("Order table: \n");
    //     err = do_query(order_crsr);
    //     ASSERT(err);
    //     printf("Order_line table: \n");
    //     err = do_query(ol_crsr);
    //     ASSERT(err);
    //     printf("New order table: \n");
    //     err = do_query(no_crsr);
    // }

    // printf("Close cursor\n");
    // Close all crsr
    ASSERT(ib_cursor_close(order_crsr));
    order_crsr = NULL;

    ASSERT(ib_cursor_close(ol_crsr));
    ol_crsr = NULL;

    ASSERT(ib_cursor_close(no_crsr));
    ol_crsr = NULL;

    // printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);

    //printf("Order table %ld-%ld initialization committed.\n\n", wh_id, dist_id);
    return DB_SUCCESS;
}

ib_err_t tpcc_db_t::init_hist_data(ib_ulint_t wh_id, ib_ulint_t dist_id, ib_ulint_t cust_id)
{
    // To do
    printf("History table %ld-%ld-%ld initialization began.\n", wh_id, dist_id, cust_id);
    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;
    auto hist_tbl = tbls[history];

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    err = open_table(dbname, hist_tbl.name, ib_trx, &crsr);
    ASSERT(err);

    //printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    ASSERT(err);

    auto tpl = ib_clust_read_tuple_create(crsr);
    // modified tuple and insert

    for (ib_ulint_t cust_id = 1; cust_id <= TPCCConfig::g_cust_per_dist; cust_id++)
    {
        std::string field;

        ASSERT(ib_col_set_value(tpl, H_C_ID, &cust_id, hist_tbl.cols[H_C_ID].len));
        ASSERT(ib_col_set_value(tpl, H_C_D_ID, &dist_id, hist_tbl.cols[H_C_D_ID].len));
        ASSERT(ib_col_set_value(tpl, H_C_W_ID, &wh_id, hist_tbl.cols[H_C_W_ID].len));
        ASSERT(ib_col_set_value(tpl, H_D_ID, &dist_id, hist_tbl.cols[H_D_ID].len));
        ASSERT(ib_col_set_value(tpl, H_W_ID, &wh_id, hist_tbl.cols[H_W_ID].len));
        ib_ulint_t val = 0;
        ASSERT(ib_col_set_value(tpl, H_DATE, &val, hist_tbl.cols[H_DATE].len));
        double amount = 10.0;
        ASSERT(ib_col_set_value(tpl, H_AMOUNT, &amount, hist_tbl.cols[H_AMOUNT].len));
        // t_history->set_row_value_int(data, H_C_ID, c_id);
        // t_history->set_row_value_int(data, H_C_D_ID, d_id);
        // t_history->set_row_value_int(data, H_C_W_ID, w_id);
        // t_history->set_row_value_int(data, H_D_ID, d_id);
        // t_history->set_row_value_int(data, H_W_ID, w_id);
        // t_history->set_row_value_int(data, H_DATE, 0);
        // t_history->set_row_value_double(data, H_AMOUNT, 10.0);

        ASSERT(ib_cursor_insert_row(crsr, tpl));
        tpl = ib_tuple_clear(tpl);
        ASSERT(err);
    }
    // if (wh_id == num_wh && dist_id == TPCCConfig::g_dist_per_ware && cust_id == TPCCConfig::g_cust_per_dist)
    // {
    //     printf("History table:\n");
    //     err = do_query(crsr);
    //     ASSERT(err);
    // }

    // printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    ASSERT(err);
    crsr = NULL;

    // printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    ASSERT(err);
    //printf("History table %ld-%ld-%ld initialization committed.\n\n", wh_id, dist_id, cust_id);
    return DB_SUCCESS;
}

void tpcc_db_t::init_permutation(ib_ulint_t *perm_c_id, ib_ulint_t wh_id)
{
    uint32_t i;
    // Init with consecutive values
    for (i = 0; i < TPCCConfig::g_cust_per_dist; i++)
        perm_c_id[i] = i + 1;

    // shuffle
    for (i = 0; i < TPCCConfig::g_cust_per_dist - 1; i++)
    {
        uint64_t j = URand(i + 1, TPCCConfig::g_cust_per_dist - 1, wh_id - 1);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}
