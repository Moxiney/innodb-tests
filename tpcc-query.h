#pragma once
#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include <cstdlib>
#include <string>



enum TPCCTxnType {
    TPCC_PAYMENT,
    TPCC_NEW_ORDER,
    TPCC_ORDER_STATUS,
    TPCC_DELIVERY,
    TPCC_STOCK_LEVEL
};

// items of new order transaction
struct Item_no {
    uint64_t ol_i_id;        // item id
    uint64_t ol_supply_w_id; // 由哪个warehouse提供，1%几率remote
    uint64_t ol_quantity;    // 数量
};

class tpcc_query {
  public:
    tpcc_query() = default;
    ~tpcc_query() = default;
    void init(uint64_t thd_id);

    TPCCTxnType type;
    /**********************************************/
    // common txn input for both payment & new-order
    /**********************************************/
    uint64_t w_id;
    uint64_t d_id;
    uint64_t c_id;
    /**********************************************/
    // txn input for payment
    /**********************************************/
    uint64_t d_w_id;
    uint64_t c_w_id;
    uint64_t c_d_id;
    std::string c_last;
    double h_amount;
    bool by_last_name;
    /**********************************************/
    // txn input for new-order
    /**********************************************/
    Item_no *items; // order-line中的每个item的信息
    bool rbk;
    bool remote;
    uint64_t ol_cnt;
    uint64_t o_entry_d;

    // Input for delivery
    uint64_t o_carrier_id;
    uint64_t ol_delivery_d;
    // for order-status

  private:
    // warehouse id to partition id mapping
    void gen_payment(uint64_t thd_id);
    void gen_new_order(uint64_t thd_id);
    void gen_order_status(uint64_t thd_id);
};