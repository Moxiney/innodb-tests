#pragma once
#include "tpcc.h"

const ib_tbl_t WAREHOUSE = {
    "WAREHOUSE",

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
    {{"W_INDEX",
      {{"W_ID", IB_INT, 8}}}}};

const ib_tbl_t DISTRICT = {
    "DISTRICT",

    {{"D_ID", IB_INT, 8},
     {"D_W_ID", IB_INT, 8},
     {"D_NAME", IB_VARCHAR, 10},
     {"D_STREET_1", IB_VARCHAR, 20},
     {"D_STREET_2", IB_VARCHAR, 20},
     {"D_CITY", IB_VARCHAR, 20},
     {"D_STATE", IB_VARCHAR, 2},
     {"D_ZIP", IB_VARCHAR, 9},
     {"D_TAX", IB_DOUBLE, 8},
     {"D_YTD", IB_DOUBLE, 8},
     {"D_NEXT_O_ID", IB_INT, 8}},
    {{"D_INDEX",
      {{"D_ID", IB_INT, 8},
       {"D_W_ID", IB_INT, 8}}}}};

const ib_tbl_t CUSTOMER = {
    "CUSTOMER",
    {{"C_ID", IB_INT, 8},
     {"C_D_ID", IB_INT, 8},
     {"C_W_ID", IB_INT, 8},
     {"C_FIRST", IB_VARCHAR, 16},
     {"C_MIDDLE", IB_VARCHAR, 2},
     {"C_LAST", IB_VARCHAR, 16},
     {"C_STREET_1", IB_VARCHAR, 20},
     {"C_STREET_2", IB_VARCHAR, 20},
     {"C_CITY", IB_VARCHAR, 20},
     {"C_STATE", IB_VARCHAR, 2},
     {"C_ZIP", IB_VARCHAR, 9},
     {"C_PHONE", IB_VARCHAR, 16},
     {"C_SINCE", IB_INT, 8},
     {"C_CREDIT", IB_VARCHAR, 2},
     {"C_CREDIT_LIM", IB_INT, 8},
     {"C_DISCOUNT", IB_DOUBLE, 8},
     {"C_BALANCE", IB_DOUBLE, 8},
     {"C_YTD_PAYMENT", IB_DOUBLE, 8},
     {"C_PAYMENT_CNT", IB_INT, 8},
     {"C_DELIVERY_CNT", IB_INT, 8},
     {"C_DATA", IB_VARCHAR, 500}},
    {{"C_INDEX",
      {{"C_ID", IB_INT, 8},
       {"C_D_ID", IB_INT, 8},
       {"C_W_ID", IB_INT, 8}}},
     {"C_LAST_INDEX",
      {{"C_LAST", IB_VARCHAR, 16},
       {"C_D_ID", IB_INT, 8},
       {"C_W_ID", IB_INT, 8}}}}};

const ib_tbl_t HISTORY = {
    "HISTORY",
    {{"H_C_ID", IB_INT, 8},
     {"H_C_D_ID", IB_INT, 8},
     {"H_C_W_ID", IB_INT, 8},
     {"H_D_ID", IB_INT, 8},
     {"H_W_ID", IB_INT, 8},
     {"H_DATE", IB_INT, 8},
     {"H_AMOUNT", IB_DOUBLE, 8},
     {"H_DATA", IB_VARCHAR, 20}},
    {}};

const ib_tbl_t NEW_ORDER = {
    "NEW_ORDER",

    {{"NO_O_ID", IB_INT, 8},
     {"NO_D_ID", IB_INT, 8},
     {"NO_W_ID", IB_INT, 8}},
    {{"NO_INDEX",
      {{"NO_O_ID", IB_INT, 8},
       {"NO_D_ID", IB_INT, 8},
       {"NO_W_ID", IB_INT, 8}}}}};

const ib_tbl_t ORDER = {
    "ORDER",

    {{"O_ID", IB_INT, 8},
     {"O_C_ID", IB_INT, 8},
     {"O_D_ID", IB_INT, 8},
     {"O_W_ID", IB_INT, 8},
     {"O_ENTRY_D", IB_INT, 8},
     {"O_CARRIER_ID", IB_INT, 8},
     {"O_OL_CNT", IB_INT, 8},
     {"O_ALL_LOCAL", IB_INT, 8}},
    {{"O_INDEX",
      {{"O_ID", IB_INT, 8},
       {"O_W_ID", IB_INT, 8},
       {"O_D_ID", IB_INT, 8}}}}};

const ib_tbl_t ORDER_LINE = {
    "ORDER_LINE",

    {{"OL_O_ID", IB_INT, 8},
     {"OL_D_ID", IB_INT, 8},
     {"OL_W_ID", IB_INT, 8},
     {"OL_NUMBER", IB_INT, 8},
     {"OL_I_ID", IB_INT, 8},
     {"OL_SUPPLY_W_ID", IB_INT, 8},
     {"OL_DELIVERY_D", IB_INT, 8},
     {"OL_QUANTITY", IB_INT, 8},
     {"OL_AMOUNT", IB_DOUBLE, 8},
     {"OL_DIST_INFO", IB_INT, 8}},
    {{"OL_INDEX",
      {{"OL_O_ID", IB_INT, 8},
       {"OL_D_ID", IB_INT, 8},
       {"OL_W_ID", IB_INT, 8},
       {"OL_NUMBER", IB_INT, 8}}}}};

const ib_tbl_t ITEM = {
    "ITEM",
    
    {{"I_ID", IB_INT, 8},
     {"I_IM_ID", IB_INT, 8},
     {"I_NAME", IB_VARCHAR, 24},
     {"I_PRICE", IB_INT, 8},
     {"I_DATA", IB_VARCHAR, 50}},
    {{"I_INDEX",
    {{"I_ID", IB_INT, 8}}}}};

const ib_tbl_t STOCK = {
    "STOCK",
    
    {{"S_I_ID", IB_INT, 8},
     {"S_W_ID", IB_INT, 8},
     {"S_QUANTITY", IB_INT, 8},
     {"S_DIST_01", IB_VARCHAR, 24},
     {"S_DIST_02", IB_VARCHAR, 24},
     {"S_DIST_03", IB_VARCHAR, 24},
     {"S_DIST_04", IB_VARCHAR, 24},
     {"S_DIST_05", IB_VARCHAR, 24},
     {"S_DIST_06", IB_VARCHAR, 24},
     {"S_DIST_07", IB_VARCHAR, 24},
     {"S_DIST_08", IB_VARCHAR, 24},
     {"S_DIST_09", IB_VARCHAR, 24},
     {"S_DIST_10", IB_VARCHAR, 24},
     {"S_YTD", IB_INT, 8},
     {"S_ORDER_CNT", IB_INT, 8},
     {"S_REMOTE_CNT", IB_INT, 8},
     {"S_DATA", IB_VARCHAR, 50}},
    {{"S_INDEX",
    {{"S_W_ID", IB_INT, 8},
     {"S_I_ID", IB_INT, 8}}}}};

const tpcc_db_t tpcc_db = {
    "tpcc_db",
    {WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, ORDER, ORDER_LINE, ITEM, STOCK}};

enum
{
    W_ID,
    W_NAME,
    W_STREET_1,
    W_STREET_2,
    W_CITY,
    W_STATE,
    W_ZIP,
    W_TAX,
    W_YTD
};
enum
{
    D_ID,
    D_W_ID,
    D_NAME,
    D_STREET_1,
    D_STREET_2,
    D_CITY,
    D_STATE,
    D_ZIP,
    D_TAX,
    D_YTD,
    D_NEXT_O_ID
};
enum
{
    C_ID,
    C_D_ID,
    C_W_ID,
    C_FIRST,
    C_MIDDLE,
    C_LAST,
    C_STREET_1,
    C_STREET_2,
    C_CITY,
    C_STATE,
    C_ZIP,
    C_PHONE,
    C_SINCE,
    C_CREDIT,
    C_CREDIT_LIM,
    C_DISCOUNT,
    C_BALANCE,
    C_YTD_PAYMENT,
    C_PAYMENT_CNT,
    C_DELIVERY_CNT,
    C_DATA
};
enum
{
    H_C_ID,
    H_C_D_ID,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,
    H_AMOUNT,
    H_DATA
};
enum
{
    NO_O_ID,
    NO_D_ID,
    NO_W_ID
};
enum
{
    O_ID,
    O_C_ID,
    O_D_ID,
    O_W_ID,
    O_ENTRY_D,
    O_CARRIER_ID,
    O_OL_CNT,
    O_ALL_LOCAL
};
enum
{
    OL_O_ID,
    OL_D_ID,
    OL_W_ID,
    OL_NUMBER,
    OL_I_ID,
    OL_SUPPLY_W_ID,
    OL_DELIVERY_D,
    OL_QUANTITY,
    OL_AMOUNT,
    OL_DIST_INFO
};
enum
{
    I_ID,
    I_IM_ID,
    I_NAME,
    I_PRICE,
    I_DATA
};
enum
{
    S_I_ID,
    S_W_ID,
    S_QUANTITY,
    S_DIST_01,
    S_DIST_02,
    S_DIST_03,
    S_DIST_04,
    S_DIST_05,
    S_DIST_06,
    S_DIST_07,
    S_DIST_08,
    S_DIST_09,
    S_DIST_10,
    S_YTD,
    S_ORDER_CNT,
    S_REMOTE_CNT,
    S_DATA
};

enum
{
    warehouse,
    district,
    customer,
    history,
    new_order,
    order,
    order_line,
    item,
    stock
};