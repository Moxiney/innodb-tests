#include "tpcc.h"
#include "tpcc-aux.h"
#include <iostream>
#include <memory>

ib_err_t tpcc_run_txn(tpcc_db_t *db, int thd_id, int &num, Barrier *barrier)
{
    // To do:
    //printf("new txn\n");
    ib_err_t err;
    auto query = std::make_unique<tpcc_query>();

    query->init(thd_id);
    while (query->type != TPCC_PAYMENT)
    {
        query->init(thd_id);
    }
    if (query->type == TPCC_PAYMENT)
    {
        printf("payment\n");
        err = run_payment(db, query.get());
    }
    else
    {
        printf("new order\n");
        err = run_new_order(db, query.get());
    }

    return err;
}

ib_err_t run_payment(tpcc_db_t *db, tpcc_query *query)
{
    // To do
    printf("wid :%ld, did: %ld, cid :%ld \n", query->w_id, query->d_id, query->c_id);
    printf("dwid :%ld, cwid: %ld, cdid :%ld, clast: %s \n", query->d_w_id, query->c_w_id, query->c_d_id, query->c_last.c_str());
    printf("hamout :%f, by_last %d \n", query->h_amount, query->by_last_name);

    // 变量定义
    ib_trx_t ib_trx;
    ib_err_t err;
    ib_crsr_t w_crsr = NULL, d_crsr = NULL, c_crsr = NULL, h_crsr = NULL;
    ib_crsr_t w_idx_crsr = NULL, d_idx_crsr = NULL, c_idx_crsr = NULL;
    ib_col_meta_t col_meta;
    int res = ~0;

    // 定义close_all_crsr函数, 关闭所有表格指针.
    auto close_all_crsrs = [&]() {
        if (w_idx_crsr != NULL)
            ASSERT(ib_cursor_close(w_idx_crsr));
        if (d_idx_crsr != NULL)
            ASSERT(ib_cursor_close(d_idx_crsr));
        if (c_idx_crsr != NULL)
            ASSERT(ib_cursor_close(c_idx_crsr));
        if (w_crsr != NULL)
            ASSERT(ib_cursor_close(w_crsr));
        if (d_crsr != NULL)
            ASSERT(ib_cursor_close(d_crsr));
        if (c_crsr != NULL)
            ASSERT(ib_cursor_close(c_crsr));
        if (h_crsr != NULL)
            ASSERT(ib_cursor_close(h_crsr));
        w_crsr = NULL;
        d_crsr = NULL;
        c_crsr = NULL;
        h_crsr = NULL;
        w_idx_crsr = NULL;
        d_idx_crsr = NULL;
        c_idx_crsr = NULL;
    };
    // 定义commit函数
    auto trx_commit = [&]() {
        printf("committed\n");
        assert(ib_trx != NULL);
        close_all_crsrs();
        ASSERT(ib_trx_commit(ib_trx));
    };
    // 定义abort函数
    auto trx_abort = [&]() {
        if (ib_trx_state(ib_trx) == IB_TRX_ACTIVE)
        {
            printf("roll backed\n");
            assert(ib_trx != NULL);
            close_all_crsrs();
            ASSERT(ib_trx_rollback(ib_trx));
        }
        else
        {
            printf("aborted\n");
            assert(ib_trx != NULL);
            close_all_crsrs();
            ASSERT(ib_trx_release(ib_trx));
        }
    };

    /* 1. 创建Transaction */
    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    /* 2. 读取Warehouse表格, w_id = _w_id; */

    // 2.1 打开Warehouse表格以及Warehouse idx表格并且上锁
    printf("Opening table warehouse\n");
    auto w_tbl = db->tbls[warehouse];
    auto w_idx = w_tbl.idxs[0];
    ASSERT(open_table(db->dbname, w_tbl.name, ib_trx, &w_crsr));
    ASSERT(ib_cursor_lock(w_crsr, IB_LOCK_IX));
    ASSERT(ib_cursor_open_index_using_name(w_crsr, w_idx.name, &w_idx_crsr));
    ASSERT(ib_cursor_set_lock_mode(w_idx_crsr, IB_LOCK_S));
    ib_cursor_set_cluster_access(w_idx_crsr);

    // 2.2 移动w_crsr直到满足w_id = _w_id
    auto w_sec_key_tpl = ib_sec_search_tuple_create(w_idx_crsr);
    assert(w_sec_key_tpl != NULL);
    ASSERT(ib_col_set_value(w_sec_key_tpl, 0, &query->w_id, w_idx.cols[0].len));

    err = ib_cursor_moveto(w_idx_crsr, w_sec_key_tpl, IB_CUR_GE, &res);
    if (err != DB_SUCCESS)
    {
        trx_abort();
        return err;
    }
    ib_tuple_delete(w_sec_key_tpl);

    // 2.3 读写该行
    // 读取W_NAME, 更新W_YTD

    std::string w_name;

    if (res == 0)
    {
        double w_ytd;
        auto old_tpl = ib_clust_read_tuple_create(w_crsr);
        auto new_tpl = ib_clust_read_tuple_create(w_crsr);
        ASSERT(ib_cursor_read_row(w_idx_crsr, old_tpl));
        ASSERT(ib_tuple_copy(new_tpl, old_tpl));

        print_tuple(stdout, old_tpl);
        ASSERT(ib_tuple_read_double(old_tpl, W_YTD, &w_ytd));
        auto w_name_cstr = ib_col_get_value(old_tpl, W_NAME);
        auto w_name_len = ib_col_get_meta(old_tpl, W_NAME, &col_meta);
        w_name.assign(static_cast<const char *>(w_name_cstr), w_name_len);

        printf("W_YTD %f, W_NAME %s\n", w_ytd, w_name.c_str());

        w_ytd += query->h_amount;

        ASSERT(ib_col_set_value(new_tpl, W_YTD, &w_ytd, w_tbl.cols[W_YTD].len));
        print_tuple(stdout, new_tpl);

        err = ib_cursor_update_row(w_idx_crsr, old_tpl, new_tpl);
        assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

        old_tpl = ib_tuple_clear(old_tpl);
        assert(old_tpl != NULL);

        new_tpl = ib_tuple_clear(new_tpl);
        assert(new_tpl != NULL);

        ib_tuple_delete(old_tpl);
        ib_tuple_delete(new_tpl);
    }
    // 2.4 关闭
    close_all_crsrs();

    /* 3. 读取District表格, d_id = _d_id, d_w_id = :w_id; */
    // 3.1 打开district表格以及district idx表格并且上锁
    printf("Opening table district\n");
    auto d_tbl = db->tbls[district];
    auto d_idx = d_tbl.idxs[0];
    ASSERT(open_table(db->dbname, d_tbl.name, ib_trx, &d_crsr));
    ASSERT(ib_cursor_lock(d_crsr, IB_LOCK_IX));
    ASSERT(ib_cursor_open_index_using_name(d_crsr, d_idx.name, &d_idx_crsr));
    ASSERT(ib_cursor_set_lock_mode(d_idx_crsr, IB_LOCK_S));
    ib_cursor_set_cluster_access(d_idx_crsr);

    // 3.2 移动d_crsr直到满足d_id = _d_id, d_w_id = _w_id
    auto d_sec_key_tpl = ib_sec_search_tuple_create(d_idx_crsr);
    assert(d_sec_key_tpl != NULL);
    ASSERT(ib_col_set_value(d_sec_key_tpl, 0, &query->d_id, d_idx.cols[0].len));
    ASSERT(ib_col_set_value(d_sec_key_tpl, 1, &query->d_w_id, d_idx.cols[1].len));

    // print_tuple(stdout, d_sec_key_tpl);

    err = ib_cursor_moveto(d_idx_crsr, d_sec_key_tpl, IB_CUR_GE, &res);
    if (err != DB_SUCCESS)
    {
        trx_abort();
        return err;
    }
    ib_tuple_delete(d_sec_key_tpl);

    // 3.3 读写该行
    // 读取D_NAME, 更新D_YTD

    std::string d_name;

    if (res == 0)
    {
        double d_ytd;
        auto old_tpl = ib_clust_read_tuple_create(d_crsr);
        auto new_tpl = ib_clust_read_tuple_create(d_crsr);
        ASSERT(ib_cursor_read_row(d_idx_crsr, old_tpl));
        ASSERT(ib_tuple_copy(new_tpl, old_tpl));

        print_tuple(stdout, old_tpl);
        ASSERT(ib_tuple_read_double(old_tpl, D_YTD, &d_ytd));
        auto d_name_cstr = ib_col_get_value(old_tpl, D_NAME);
        auto d_name_len = ib_col_get_meta(old_tpl, D_NAME, &col_meta);
        d_name.assign(static_cast<const char *>(d_name_cstr), d_name_len);

        printf("D_YTD %f, D_NAME %s\n", d_ytd, d_name.c_str());

        d_ytd += query->h_amount;

        ASSERT(ib_col_set_value(new_tpl, D_YTD, &d_ytd, d_tbl.cols[D_YTD].len));
        print_tuple(stdout, new_tpl);

        err = ib_cursor_update_row(d_idx_crsr, old_tpl, new_tpl);
        assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

        old_tpl = ib_tuple_clear(old_tpl);
        assert(old_tpl != NULL);

        new_tpl = ib_tuple_clear(new_tpl);
        assert(new_tpl != NULL);

        ib_tuple_delete(old_tpl);
        ib_tuple_delete(new_tpl);
    }
    close_all_crsrs();

    /* 4. 读取Customer表格, c_id = _c_id, c_w_id = :w_id; */
    // 4.1 打开customer表格以及customer idx表格并且上锁
    printf("Opening table customer\n");
    auto c_tbl = db->tbls[customer];
    auto idx_id = query->by_last_name ? 1 : 0;
    auto c_idx = c_tbl.idxs[idx_id];

    ASSERT(open_table(db->dbname, c_tbl.name, ib_trx, &c_crsr));
    ASSERT(ib_cursor_lock(c_crsr, IB_LOCK_IX));
    printf("customer index name %s\n", c_idx.name);
    ASSERT(ib_cursor_open_index_using_name(c_crsr, c_idx.name, &c_idx_crsr));
    ASSERT(ib_cursor_set_lock_mode(c_idx_crsr, IB_LOCK_S));
    ib_cursor_set_cluster_access(c_idx_crsr);

    // 3.2 移动c_crsr直到满足c_id = _c_id(c_last = _c_last), c_d_id = _c_d_id, c_w_id = _c_w_id
    auto c_sec_key_tpl = ib_sec_search_tuple_create(c_idx_crsr);
    assert(c_sec_key_tpl != NULL);
    if (query->by_last_name)
    {
        ASSERT(ib_col_set_value(c_sec_key_tpl, 0, query->c_last.c_str(), query->c_last.size()));
    }
    else
    {
        ASSERT(ib_col_set_value(c_sec_key_tpl, 0, &query->c_id, c_idx.cols[0].len));
    }
    ASSERT(ib_col_set_value(c_sec_key_tpl, 1, &query->c_d_id, c_idx.cols[1].len));
    ASSERT(ib_col_set_value(c_sec_key_tpl, 2, &query->c_w_id, c_idx.cols[2].len));
    print_tuple(stdout, c_sec_key_tpl);
    
    auto ncols = ib_tuple_get_n_cols(c_sec_key_tpl);
    if (ncols == 4) {
        printf("ncols %ld\n", ncols);
    }
    
    

    err = ib_cursor_moveto(c_idx_crsr, c_sec_key_tpl, IB_CUR_GE, &res);
    if (err != DB_SUCCESS || res != 0)
    {
        trx_abort();
        return err;
    }
    ib_tuple_delete(c_sec_key_tpl);


    std::string c_credit;
    if (res == 0)
    {
        double c_balance;
        double c_ytd_payment;
        ib_ulint_t c_payment_cnt;

        auto old_tpl = ib_clust_read_tuple_create(c_crsr);
        auto new_tpl = ib_clust_read_tuple_create(c_crsr);
        ASSERT(ib_cursor_read_row(c_idx_crsr, old_tpl));
        ASSERT(ib_tuple_copy(new_tpl, old_tpl));

        print_tuple(stdout, old_tpl);

        ASSERT(ib_tuple_read_double(old_tpl, C_BALANCE, &c_balance));
        ASSERT(ib_tuple_read_double(old_tpl, C_YTD_PAYMENT, &c_ytd_payment));
        ASSERT(ib_tuple_read_u64(old_tpl, C_PAYMENT_CNT, &c_payment_cnt));

        auto c_credit_cstr = ib_col_get_value(old_tpl, C_CREDIT);
        auto c_credit_len = ib_col_get_meta(old_tpl, C_CREDIT, &col_meta);
        c_credit.assign(static_cast<const char *>(c_credit_cstr), c_credit_len);
        

        c_balance -= query->h_amount;
        c_ytd_payment -= query->h_amount;
        c_payment_cnt += 1;

        ASSERT(ib_col_set_value(new_tpl, C_BALANCE, &c_balance, c_tbl.cols[C_BALANCE].len));
        ASSERT(ib_col_set_value(new_tpl, C_YTD_PAYMENT, &c_ytd_payment, c_tbl.cols[C_BALANCE].len));
        ASSERT(ib_col_set_value(new_tpl, C_PAYMENT_CNT, &c_payment_cnt, c_tbl.cols[C_BALANCE].len));

        print_tuple(stdout, new_tpl);

        err = ib_cursor_update_row(c_idx_crsr, old_tpl, new_tpl);
        assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

        old_tpl = ib_tuple_clear(old_tpl);
        assert(old_tpl != NULL);

        new_tpl = ib_tuple_clear(new_tpl);
        assert(new_tpl != NULL);

        ib_tuple_delete(old_tpl);
        ib_tuple_delete(new_tpl);

    }
    close_all_crsrs();

    /* 4. Insert into History*/
    std::string h_data;
    h_data += w_name;
    int length = h_data.size();
    if (length > 10)
        length = 10;
    h_data = h_data.substr(0, length);
    h_data += "    ";
    h_data += d_name;
    h_data = h_data.substr(0, 24);

    printf("Opening table history\n");
    auto h_tbl = db->tbls[history];
    ASSERT(open_table(db->dbname, h_tbl.name, ib_trx, &h_crsr));
    ASSERT(ib_cursor_lock(h_crsr, IB_LOCK_IX));

    auto h_tpl = ib_clust_read_tuple_create(h_crsr);

    ASSERT(ib_col_set_value(h_tpl, H_C_ID, &query->c_id, h_tbl.cols[H_C_ID].len));    
    ASSERT(ib_col_set_value(h_tpl, H_C_W_ID, &query->c_w_id, h_tbl.cols[H_C_W_ID].len));
    ASSERT(ib_col_set_value(h_tpl, H_D_ID, &query->d_id, h_tbl.cols[H_D_ID].len));
    ASSERT(ib_col_set_value(h_tpl, H_W_ID, &query->w_id, h_tbl.cols[H_W_ID].len));
    ib_ulint_t date = 2019;
    ASSERT(ib_col_set_value(h_tpl, H_DATE, &date, h_tbl.cols[H_DATE].len));
    ASSERT(ib_col_set_value(h_tpl, H_AMOUNT, &query->h_amount, h_tbl.cols[H_AMOUNT].len));
    ASSERT(ib_col_set_value(h_tpl, H_DATA, h_data.c_str(), h_data.size()));

    print_tuple(stdout, h_tpl);

    ASSERT(ib_cursor_insert_row(h_crsr, h_tpl));
    h_tpl = ib_tuple_clear(h_tpl);

    // Commit
    trx_commit();
    return DB_SUCCESS;
}

ib_err_t run_new_order(tpcc_db_t *db, tpcc_query *query)
{
    // To do
    return DB_SUCCESS;
}
