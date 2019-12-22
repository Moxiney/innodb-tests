#include "tpcc.h"

#include <iostream>
#include <memory>

ib_err_t tpcc_run_txn(tpcc_db_t db, int thd_id, int &num, Barrier *barrier)
{
    // To do:
    //printf("new txn\n");
    ib_err_t err;
    auto query = std::make_unique<tpcc_query>();

    query->init(thd_id);
    while (query->type != TPCC_PAYMENT) {
        query->init(thd_id);
    }
    if (query->type == TPCC_PAYMENT) {
        printf("payment\n");
        err = run_payment(query.get());
    } else {
        printf("new order\n");
        err = run_new_order(query.get());
    }

    return err;
}

ib_err_t run_payment(tpcc_query *query) {
    // To do
    printf("wid :%ld, did: %ld, cid :%ld \n", query->w_id, query->d_id, query->c_id);
    printf("dwid :%ld, cwid: %ld, cdid :%ld, clast: %s \n", query->d_w_id, query->c_w_id, query->c_d_id, query->c_last.c_str());
    printf("hamout :%f, by_last %d \n", query->h_amount, query->by_last_name);
    return DB_SUCCESS;
}

ib_err_t run_new_order(tpcc_query *query) {
    // To do
    return DB_SUCCESS;
}
