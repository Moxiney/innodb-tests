#pragma once
#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include <vector>
// 定义col, table等数据结构, 以及一些辅助函数.

struct ib_col_t {
    char* name;
    ib_col_type_t type;
    ib_ulint_t len;
};


struct ib_tbl_t {
    char* table_name;
    char* index_name;
    std::vector<ib_col_t> cols;
    std::vector<ib_col_t> index_cols;
    ib_err_t create_table(const char* dbname);
    ib_err_t drop_table(const char* dbname);
};

struct ib_db_t {
    char *dbname;
    std::vector<ib_tbl_t> tbls;
};


