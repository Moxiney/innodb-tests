#pragma once
#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include <vector>
#include <assert.h>
// 定义col, table等数据结构, 以及一些辅助函数.

extern int done;
extern int num_wh;

#define ASSERT(err) assert(err == DB_SUCCESS)

class TPCCConfig {
  public:
    /*
     * 以下参数仅供YCSB和TPCC等workload使用
     ***************************************/

    /*
     * TPCC使用
     * */
    // TODO: 暂时调小测试正确性
    const static int g_dist_per_ware = 10; // 每个仓库给几个地区供货
    const static int g_cust_per_dist = 5; // 每个地区客户数量
    const static int g_max_items = 10;    // 供应的商品种类

    const static int firstname_minlen = 8;
    const static int firstname_len = 16;
    const static int lastname_len = 16;

    const static int g_perc_neworder = 45; // new_order所占比例
};

struct ib_col_t {
    const char* name;
    ib_col_type_t type;
    ib_ulint_t len;
};


struct ib_tbl_t {
    const char* name;
    const char* idx_name;
    std::vector<ib_col_t> cols;
    std::vector<ib_col_t> idx_cols;
    ib_err_t create_table(const char* dbname);
    ib_err_t drop_table(const char* dbname);
};

class tpcc_db_t {
public:
    const char *dbname;
    std::vector<ib_tbl_t> tbls;
    int num_wh;

    tpcc_db_t() = default;
    ~tpcc_db_t() = default;

    ib_err_t init();
    ib_err_t shutdown();
    ib_err_t init_tables_data();
private:
    ib_err_t init_item_data();
    ib_err_t init_wh_data(ib_ulint_t wh_id);
    ib_err_t init_dist_data(ib_ulint_t wh_id);
    ib_err_t init_stock_data(ib_ulint_t wh_id);
    ib_err_t init_cust_data(ib_ulint_t wh_id, ib_ulint_t dist_id);
    ib_err_t init_order_data(ib_ulint_t wh_id, ib_ulint_t dist_id);
    ib_err_t init_hist_data(ib_ulint_t wh_id, ib_ulint_t dist_id, ib_ulint_t cust_id);

    void init_permutation(ib_ulint_t *perm_c_id, ib_ulint_t wh_id);
};

ib_err_t tpcc_run_txn(tpcc_db_t tpcc_db);


