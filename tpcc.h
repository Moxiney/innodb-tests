#pragma once
#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include <vector>
// 定义col, table等数据结构, 以及一些辅助函数.

struct ib_col_t
{
    char *name;
    ib_col_type_t type;
    ib_ulint_t len;
};

ib_col_t test_col = {
    "col1",
    IB_VARCHAR,
    8};

struct ib_tbl_t
{
    char *table_name;
    char *index_name;
    std::vector<ib_col_t> cols;
    std::vector<ib_col_t> index_cols;
    ib_err_t create_table(const char *dbname);
};

const ib_tbl_t WAREHOUSE =
    {
        "WAREHOUSE",
        "W_INDEX",
        {
            {"W_ID", IB_INT, 8},
            {"W_NAME", IB_VARCHAR, 10},
            {"W_STREET_1", IB_VARCHAR, 20},
            {"W_STREET_2", IB_VARCHAR, 20},
            {"W_CITY", IB_VARCHAR, 20},
            {"W_STATE", IB_VARCHAR, 2},
            {"W_ZIP", IB_VARCHAR, 9},
            {"W_TAX", IB_DOUBLE, 8},
            {"W_YTD", IB_DOUBLE, 8},
        },
        {
            {"W_ID", IB_INT, 8}
        }}

const ib_tbl_t DISTRICT;
