#include "tpcc-query.h"
#include "tpcc-helper.h"
#include "tpcc.h"
#include <assert.h>
#include <mm_malloc.h>

int min(int a, int b) {
    return a < b? a: b;
} 

void tpcc_query::init(uint64_t thd_id) {
    int x = rand() % 100;
    if (x < TPCCConfig::g_perc_neworder)
        gen_new_order(thd_id);
    else
        gen_payment(thd_id);
}

void tpcc_query::gen_payment(uint64_t thd_id) {
    type = TPCC_PAYMENT;

    // 首先随机warehouse的id
    w_id = URand(1, num_wh, thd_id % num_wh);

    d_w_id = w_id;

    // 随机district的id
    d_id = URand(1, TPCCConfig::g_dist_per_ware, w_id - 1);

    // 金额
    h_amount = URand(1, 5000, w_id - 1);

    int x = URand(1, 100, w_id - 1);
    int y = URand(1, 100, w_id - 1);

    if (x <= 85) {
        // home warehouse
        c_d_id = d_id;
        c_w_id = w_id;
    } else {
        // remote warehouse
        c_d_id = URand(1, TPCCConfig::g_dist_per_ware, w_id - 1);
        if (num_wh > 1) {
            // 随机一个remote warehouse
            while ((c_w_id = URand(1, num_wh, w_id - 1)) == w_id) {
            }
        } else
            c_w_id = w_id;
    }
    if (y <= 60) {
        // by last name
        by_last_name = true;
        c_last = Lastname(NURand(255, 0, 999, w_id - 1));
    } else {
        // by cust id
        by_last_name = false;
        c_id = NURand(1023, 1, TPCCConfig::g_cust_per_dist, w_id - 1);
    }
}

void tpcc_query::gen_new_order(uint64_t thd_id) {
    type = TPCC_NEW_ORDER;

    // 随机warehouse id
    w_id = URand(1, num_wh, thd_id % num_wh);

    // 随机district id和customer id
    d_id = URand(1, TPCCConfig::g_dist_per_ware, w_id - 1);
    c_id = NURand(1023, 1, TPCCConfig::g_cust_per_dist, w_id - 1);

    rbk = URand(1, 100, w_id - 1);
    ol_cnt = URand(5, 15, w_id - 1); // order-line数量
    o_entry_d = 2013;
    items = (Item_no *)_mm_malloc(sizeof(Item_no) * ol_cnt, 64);
    remote = false;

    for (int oid = 0; oid < ol_cnt; oid++) {
        // 生成item id
        items[oid].ol_i_id =
            NURand(8191, 1, TPCCConfig::g_max_items, w_id - 1);
        int x = URand(1, 100, w_id - 1);
        if (x > 1 || num_wh == 1)
            items[oid].ol_supply_w_id = w_id;
        else {
            // remote warehouse
            while ((items[oid].ol_supply_w_id = URand(1, num_wh, w_id - 1)) ==
                   w_id) {
            }
            remote = true;
        }
        items[oid].ol_quantity = URand(1, 10, w_id - 1);
    }
    // Remove duplicate items
    for (int i = 0; i < ol_cnt; i++) {
        for (int j = 0; j < i; j++) {
            if (items[i].ol_i_id == items[j].ol_i_id) {
                for (int k = i; k < ol_cnt - 1; k++)
                    items[k] = items[k + 1];
                ol_cnt--;
                i--;
            }
        }
    }
    for (int i = 0; i < ol_cnt; i++)
        for (int j = 0; j < i; j++)
            assert(items[i].ol_i_id != items[j].ol_i_id);
}

void tpcc_query::gen_order_status(uint64_t thd_id) {
    type = TPCC_ORDER_STATUS;
    // 随机warehouse id
    w_id = URand(1, num_wh, thd_id % num_wh);
    d_id = URand(1, TPCCConfig::g_dist_per_ware, w_id - 1);
    c_w_id = w_id;
    c_d_id = d_id;

    int y = URand(1, 100, w_id - 1);
    if (y <= 60) {
        // by last name
        by_last_name = true;
        c_last = Lastname(NURand(255, 0, 999, w_id - 1));
    } else {
        // by cust id
        by_last_name = false;
        c_id = NURand(1023, 1, TPCCConfig::g_cust_per_dist, w_id - 1);
    }
}