#pragma once
#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include <vector>
// 定义col, table等数据结构, 以及一些辅助函数.

struct ib_col_t {
    char* name;
    ib_col_type_t type;
    ib_ulint_t len;
};

ib_col_t test_col = {
    "col1",
    IB_VARCHAR,
    8
};

struct ib_tbl_t {
    char* table_name;
    char* index_name;
    std::vector<ib_col_t> cols;
    std::vector<ib_col_t> index_cols;
    ib_err_t create_table(const char* dbname);
};

const ib_tbl_t WAREHOUSE = {
    "WAREHOUSE",
    "W_INDEX",
    {
        { "W_ID", IB_INT, 8 },
        { "W_NAME", IB_VARCHAR, 10 },
        { "W_STREET_1", IB_VARCHAR, 20 },
        { "W_STREET_2", IB_VARCHAR, 20 },
        { "W_CITY", IB_VARCHAR, 20 },
        { "W_STATE", IB_VARCHAR, 2 },
        { "W_ZIP", IB_VARCHAR, 9 },
        { "W_TAX", IB_DOUBLE, 8 },
        { "W_YTD", IB_DOUBLE, 8 },
    },
    { { "W_ID", IB_INT, 8 } }
}

const ib_tbl_t DISTRICT= { 
    "DISTRICT", 
    "D_INDEX", 
    {
        { "D_ID", IB_INT, 8 },
        { "D_W_ID", IB_INT, 8 },
        { "D_NAME", IB_VARCHAR, 10 },
        { "D_STREET_1", IB_VARCHAR, 20 },
        { "D_STREET_2", IB_VARCHAR, 20 },
        { "D_CITY", IB_VARCHAR, 20 },
        { "D_STATE", IB_VARCHAR, 2 },
        { "D_ZIP", IB_VARCHAR, 9 },
        { "D_TAX", IB_DOUBLE, 8 },
        { "D_YTD", IB_DOUBLE, 8 },
        { "D_NEXT_O_ID", IB_INT, 8 }
    },
    {
        { "D_ID", IB_INT, 8 },
        { "D_W_ID", IB_INT, 8 }
    }
}

const ib_tbl_t CUSTOMER= { 
    "CUSTOMER", 
    "C_INDEX", 
    {
        { "C_ID", IB_INT, 8 },
        { "C_D_ID", IB_INT, 8 },
        { "C_W_ID", IB_INT, 8 },
        { "C_FIRST", IB_VARCHAR, 16 },
        { "C_MIDDLE", IB_VARCHAR, 2 },
        { "C_LAST", IB_VARCHAR, 16 },
        { "C_STREET_1", IB_VARCHAR, 20 },
        { "C_STREET_2", IB_VARCHAR, 20 },
        { "C_CITY", IB_VARCHAR, 20 },
        { "C_STATE", IB_VARCHAR, 2 },
        { "C_ZIP", IB_VARCHAR, 9 },
        { "C_PHONE", IB_VARCHAR, 16 },
        { "C_SINCE", IB_INT, 8 },
        { "C_CREDIT", IB_VARCHAR, 2 },
        { "C_CREDIT_LIM", IB_INT, 8 },
        { "C_DISCOUNT", IB_INT, 8 },
        { "C_BALANCE", IB_DOUBLE, 8 },
        { "C_YTD_PAYMENT", IB_DOUBLE, 8 },
        { "C_PAYMENT_CNT", IB_INT, 8 },
        { "C_DELIVERY_CNT", IB_INT, 8 },
        { "C_DATA", IB_VARCHAR, 500 }
    },
    {
        { "C_ID", IB_INT, 8 },
        { "C_D_ID", IB_INT, 8 },
        { "C_W_ID", IB_INT, 8 }
    }
}

const ib_tbl_t HISTORY= { 
    "HISTORY", 
    "H_INDEX", 
    {
        { "H_C_ID", IB_INT, 8 },
        { "H_C_D_ID", IB_INT, 8 },
        { "H_C_W_ID", IB_INT, 8 },
        { "H_D_ID", IB_INT, 8 },
        { "H_W_ID", IB_INT, 8 },
        { "H_DATE", IB_INT, 8 },
        { "H_AMOUNT", IB_DOUBLE, 8 },
        { "H_DATA", IB_VARCHAR, 20 }
    },
    {}
}

const ib_tbl_t NEW-ORDER= { 
    "NEW-ORDER", 
    "NO_INDEX", 
    {
        { "NO_O_ID", IB_INT, 8 },
        { "NO_D_ID", IB_INT, 8 },
        { "NO_W_ID", IB_INT, 8 }
    },
    {
        { "NO_O_ID", IB_INT, 8 },
        { "NO_D_ID", IB_INT, 8 },
        { "NO_W_ID", IB_INT, 8 }
    }
}

const ib_tbl_t ORDER= { 
    "ORDER", 
    "O_INDEX", 
    {
        { "O_ID", IB_INT, 8 },
        { "O_C_ID", IB_INT, 8 },
        { "O_D_ID", IB_INT, 8 },
        { "O_W_ID", IB_INT, 8 },
        { "O_ENTRY_D", IB_INT, 8 },
        { "O_CARRIER_ID", IB_INT, 8 },
        { "O_OL_CNT", IB_INT, 8 },
        { "O_ALL_LOCAL", IB_INT, 8 }
    },
    {
        { "O_ID", IB_INT, 8 },
        { "O_W_ID", IB_INT, 8 },
        { "O_D_ID", IB_INT, 8 }
    }
}

const ib_tbl_t ORDER-LINE= { 
    "ORDER-LINE", 
    "OL_INDEX", 
    {
        { "OL_O_ID", IB_INT, 8 },
        { "OL_D_ID", IB_INT, 8 },
        { "OL_D_ID", IB_INT, 8 },
        { "OL_NUMBER", IB_INT, 8 },
        { "OL_I_ID", IB_INT, 8 },
        { "OL_SUPPLY_W_ID", IB_INT, 8 },
        { "OL_DELIVERY_D", IB_INT, 8 },
        { "OL_QUANTITY", IB_INT, 8 },
        { "OL_AMOUNT", IB_DOUBLE, 8 },
        { "OL_DIST_INFO", IB_INT, 8 }
    },
    {
        // To Do
    }
}

const ib_tbl_t ITEM= { 
    "ITEM", 
    "I_INDEX", 
    {
        { "I_ID", IB_INT, 8 },
        { "I_IM_ID", IB_INT, 8 },
        { "I_NAME", IB_VARCHAR, 24 },
        { "I_PRICE", IB_INT, 8 },
        { "I_DATA", IB_VARCHAR, 50 }
    },
    {
        { "I_ID", IB_INT, 8 }
    }
}

const ib_tbl_t STOCK= { 
    "STOCK", 
    "S_INDEX", 
    {
        { "S_I_ID", IB_INT, 8 },
        { "S_W_ID", IB_INT, 8 },
        { "S_QUANTITY", IB_INT, 8 },
        { "S_DIST_01", IB_VARCHAR, 24 },
        { "S_DIST_02", IB_VARCHAR, 24 },
        { "S_DIST_03", IB_VARCHAR, 24 },
        { "S_DIST_04", IB_VARCHAR, 24 },
        { "S_DIST_05", IB_VARCHAR, 24 },
        { "S_DIST_06", IB_VARCHAR, 24 },
        { "S_DIST_07", IB_VARCHAR, 24 },
        { "S_DIST_08", IB_VARCHAR, 24 },
        { "S_DIST_09", IB_VARCHAR, 24 },
        { "S_DIST_10", IB_VARCHAR, 24 },
        { "S_YTD", IB_INT, 8 }
    },
    {
        { "S_W_ID", IB_INT, 8 },
        { "S_I_ID", IB_INT, 8 }
    }
}
