#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <string>
#include <iostream>
#include <thread>

#include "ycsb.h"
#include "common.h"

int init_table_size = 10;
#define COL_LEN 10

typedef struct row_t
{
    char F0[COL_LEN];
    char F1[COL_LEN];
    char F2[COL_LEN];
    char F3[COL_LEN];
    char F4[COL_LEN];
    char F5[COL_LEN];
    char F6[COL_LEN];
    char F7[COL_LEN];
    char F8[COL_LEN];
    char F9[COL_LEN];
} row_t;

static ib_err_t
create_table(
    /*=========*/
    const char *dbname, /*!< in: database name */
    const char *name)   /*!< in: table name */
{
    ib_trx_t ib_trx;
    ib_id_t table_id = 0;
    ib_err_t err = DB_SUCCESS;
    ib_tbl_sch_t ib_tbl_sch = NULL;
    ib_idx_sch_t ib_idx_sch = NULL;
    char table_name[IB_MAX_TABLE_NAME_LEN];

    snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
    printf("Creating %s\n", table_name);

    /* Pass a table page size of 0, ie., use default page size. */
    err = ib_table_schema_create(
        table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

    assert(err == DB_SUCCESS);

    for (int i = 0; i < 10; i++)
    {
        char col_name[COL_LEN];
        snprintf(col_name, COL_LEN, "F%d", i);

        err = ib_table_schema_add_col(
            ib_tbl_sch, col_name,
            IB_VARCHAR, IB_COL_NONE, 0, COL_LEN - 1);
        printf("Add column %s\n", col_name);

        assert(err == DB_SUCCESS);
    }

    err = ib_table_schema_add_index(ib_tbl_sch, "F0", &ib_idx_sch);
    assert(err == DB_SUCCESS);

    /* Set prefix length to 0. */
    err = ib_index_schema_add_col(ib_idx_sch, "F0", 0);
    assert(err == DB_SUCCESS);

    err = ib_index_schema_set_clustered(ib_idx_sch);
    assert(err == DB_SUCCESS);

    /* create table */
    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    err = ib_schema_lock_exclusive(ib_trx);
    assert(err == DB_SUCCESS);

    err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
    assert(err == DB_SUCCESS);

    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);

    if (ib_tbl_sch != NULL)
    {
        ib_table_schema_delete(ib_tbl_sch);
    }

    return (err);
}

/*********************************************************************
Open a table and return a cursor for the table. */
static ib_err_t
open_table(
    /*=======*/
    const char *dbname, /*!< in: database name */
    const char *name,   /*!< in: table name */
    ib_trx_t ib_trx,    /*!< in: transaction */
    ib_crsr_t *crsr)    /*!< out: innodb cursor */
{
    ib_err_t err = DB_SUCCESS;
    char table_name[IB_MAX_TABLE_NAME_LEN];

    snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

    err = ib_cursor_open_table(table_name, ib_trx, crsr);
    assert(err == DB_SUCCESS);

    return (err);
}

ib_err_t gen_tuple(ib_tpl_t &tpl, int pkey)
{
    char pkey_str[COL_LEN] = "";
    snprintf(pkey_str, COL_LEN, "%d", pkey);
    auto err = ib_col_set_value(tpl, 0, pkey_str, COL_LEN);
    assert(err == DB_SUCCESS);

    printf("%s ", pkey_str);

    for (int i = 1; i < 10; i++)
    {
        char val_str[COL_LEN] = "";
        snprintf(val_str, COL_LEN, "%d", i);
        auto err = ib_col_set_value(tpl, i, val_str, COL_LEN);
        assert(err == DB_SUCCESS);

        printf("%s ", val_str);
    }

    return err;
}

static ib_err_t
insert_rows(
    /*========*/
    ib_crsr_t crsr) /*!< in, out: cursor to use for write */
{
    row_t *row;
    ib_tpl_t tpl = NULL;
    ib_err_t err = DB_ERROR;

    tpl = ib_clust_read_tuple_create(crsr);
    assert(tpl != NULL);

    for (int i = 0; i < init_table_size; i++)
    {
        // gen_tuple
        gen_tuple(tpl, i);

        err = ib_cursor_insert_row(crsr, tpl);
        assert(err == DB_SUCCESS);

        tpl = ib_tuple_clear(tpl);
        assert(tpl != NULL);

        printf("Add tuple %d\n", i);
    }

    if (tpl != NULL)
    {
        ib_tuple_delete(tpl);
    }

    return (err);
}



/*********************************************************************
SELECT * FROM T; */
static ib_err_t
do_query(
    /*=====*/
    ib_crsr_t crsr)
{
    ib_err_t err;
    ib_tpl_t tpl;

    tpl = ib_clust_read_tuple_create(crsr);
    assert(tpl != NULL);

    err = ib_cursor_first(crsr);
    assert(err == DB_SUCCESS);

    while (err == DB_SUCCESS)
    {
        err = ib_cursor_read_row(crsr, tpl);

        assert(err == DB_SUCCESS || err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND);

        if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX)
        {
            break;
        }

        print_tuple(stdout, tpl);

        err = ib_cursor_next(crsr);

        assert(err == DB_SUCCESS || err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND);

        tpl = ib_tuple_clear(tpl);
        assert(tpl != NULL);
    }

    if (tpl != NULL)
    {
        ib_tuple_delete(tpl);
    }

    if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX)
    {
        err = DB_SUCCESS;
    }
    return (err);
}

ib_err_t ib_col_set_value(ib_tpl_t &tpl, ib_ulint_t col, int val) {
    char val_str[COL_LEN] = "";
    snprintf(val_str, COL_LEN, "%d", val);
    auto err = ib_col_set_value(tpl, col, val_str, COL_LEN);
    assert(err == DB_SUCCESS);

    return err;
}

ib_err_t ycsb_init(
    const char *dbname,
    const char *name)
{
    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;

    printf("Create table\n");
    err = create_table(dbname, name);
    assert(err == DB_SUCCESS);

    printf("Begin transaction\n");
    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    printf("Open cursor\n");
    err = open_table(dbname, name, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    printf("Insert rows\n");
    err = insert_rows(crsr);
    assert(err == DB_SUCCESS);

    printf("Query table\n");
    err = do_query(crsr);
    assert(err == DB_SUCCESS);

    printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    assert(err == DB_SUCCESS);
    crsr = NULL;

    printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);

    return err;
}

ib_err_t update_tuple(ib_crsr_t	crsr, int pkey)
{
    ib_err_t err;
    ib_crsr_t index_crsr;
    ib_tpl_t	sec_key_tpl;
    int		res = ~0;

    /* Open the index. */
    err = ib_cursor_open_index_using_name(crsr, "F0", &index_crsr);
	assert(err == DB_SUCCESS);

    /* Set the record lock mode */
	err = ib_cursor_set_lock_mode(index_crsr, IB_LOCK_X);
	assert(err == DB_SUCCESS);

    /* Since we will be updating the clustered index record, set the
	need to access clustered index flag in the cursor. */
	ib_cursor_set_cluster_access(index_crsr);

	/* Create a tuple for searching the secondary index. */
	sec_key_tpl = ib_sec_search_tuple_create(index_crsr);
	assert(sec_key_tpl != NULL);

    char pkey_str[COL_LEN] = "";
    snprintf(pkey_str, COL_LEN, "%d", pkey);
    err = ib_col_set_value(sec_key_tpl, 0, pkey_str, COL_LEN);
	assert(err == DB_SUCCESS);

    print_tuple(stdout, sec_key_tpl);

    /* Search for the key using the cluster index (PK) */
    err = ib_cursor_moveto(index_crsr, sec_key_tpl, IB_CUR_GE, &res);
	assert(err == DB_SUCCESS
	       || err == DB_END_OF_INDEX
	       || err == DB_RECORD_NOT_FOUND);

    ib_tuple_delete(sec_key_tpl);

    /* Match found */
    if (res == 0) {
        printf("find tuple %d\n", pkey);
        int		l;
		char*		ptr;
		const char*	first;
		ib_ulint_t	data_len;
		ib_col_meta_t	col_meta;
		ib_ulint_t	first_len;
		ib_tpl_t	old_tpl = NULL;
		ib_tpl_t	new_tpl = NULL;
        RandomGenerator rnd;

        old_tpl = ib_clust_read_tuple_create(crsr);
		assert(old_tpl != NULL);

		new_tpl = ib_clust_read_tuple_create(crsr);
		assert(new_tpl != NULL);

        err = ib_cursor_read_row(index_crsr, old_tpl);
		assert(err == DB_SUCCESS);

        err = ib_tuple_copy(new_tpl, old_tpl);
        print_tuple(stdout, new_tpl);

        // Set update random column
        auto col = rnd.randomInt() % 9 + 1;
        // char val_str[COL_LEN] = "";
        // snprintf(val_str, COL_LEN, "%d", 0);
        // err = ib_col_set_value(new_tpl, col, val_str, COL_LEN);
        
        err = ib_col_set_value(new_tpl, col, 0);
        assert(err == DB_SUCCESS);
        print_tuple(stdout, new_tpl);

        err = ib_cursor_update_row(index_crsr, old_tpl, new_tpl);
		assert(err == DB_SUCCESS || err == DB_DUPLICATE_KEY);

        /* Reset the old and new tuple instances. */
        old_tpl = ib_tuple_clear(old_tpl);
		assert(old_tpl != NULL);

		new_tpl = ib_tuple_clear(new_tpl);
		assert(new_tpl != NULL);

        ib_tuple_delete(old_tpl);
		ib_tuple_delete(new_tpl);
    }

    /* Close index*/
    err = ib_cursor_close(index_crsr);
	assert(err == DB_SUCCESS);

    

    return DB_SUCCESS;
}

ib_err_t read_tuple(ib_crsr_t	crsr, int pkey)
{
    ib_err_t err;
    ib_crsr_t index_crsr;
    ib_tpl_t	sec_key_tpl;
    int		res = ~0;

    /* Open the index. */
    err = ib_cursor_open_index_using_name(crsr, "F0", &index_crsr);
	assert(err == DB_SUCCESS);

    /* Set the record lock mode */
	err = ib_cursor_set_lock_mode(index_crsr, IB_LOCK_X);
	assert(err == DB_SUCCESS);

    /* Since we will be updating the clustered index record, set the
	need to access clustered index flag in the cursor. */
	ib_cursor_set_cluster_access(index_crsr);

	/* Create a tuple for searching the secondary index. */
	sec_key_tpl = ib_sec_search_tuple_create(index_crsr);
	assert(sec_key_tpl != NULL);

    char pkey_str[COL_LEN] = "";
    snprintf(pkey_str, COL_LEN, "%d", pkey);
    err = ib_col_set_value(sec_key_tpl, 0, pkey_str, COL_LEN);
	assert(err == DB_SUCCESS);

    print_tuple(stdout, sec_key_tpl);

    /* Search for the key using the cluster index (PK) */
    err = ib_cursor_moveto(index_crsr, sec_key_tpl, IB_CUR_GE, &res);
	assert(err == DB_SUCCESS
	       || err == DB_END_OF_INDEX
	       || err == DB_RECORD_NOT_FOUND);

    ib_tuple_delete(sec_key_tpl);

    /* Match found */
    if (res == 0) {
        printf("find tuple %d\n", pkey);
        int		l;
		char*		ptr;
		const char*	first;
		ib_ulint_t	data_len;
		ib_col_meta_t	col_meta;
		ib_ulint_t	first_len;
		ib_tpl_t	old_tpl = NULL;
		ib_tpl_t	new_tpl = NULL;

        old_tpl = ib_clust_read_tuple_create(crsr);
		assert(old_tpl != NULL);

		new_tpl = ib_clust_read_tuple_create(crsr);
		assert(new_tpl != NULL);

        err = ib_cursor_read_row(index_crsr, old_tpl);
		assert(err == DB_SUCCESS);

        print_tuple(stdout, old_tpl);


        /* Reset the old and new tuple instances. */
        old_tpl = ib_tuple_clear(old_tpl);
		assert(old_tpl != NULL);

		new_tpl = ib_tuple_clear(new_tpl);
		assert(new_tpl != NULL);

        ib_tuple_delete(old_tpl);
		ib_tuple_delete(new_tpl);
    }

    /* Close index*/
    err = ib_cursor_close(index_crsr);
	assert(err == DB_SUCCESS);

    

    return DB_SUCCESS;
}

ib_err_t ycsb_run_txn(
    const char *dbname,
    const char *name)
{
    ib_err_t err;
    ib_crsr_t crsr;
    ib_trx_t ib_trx;

    int read_ratio = 50;
    RandomGenerator rnd;

    // barrier->wait()
    // int round = 10;
    // while (round)
    // {
    //     round--;
    //     int query_num = rnd.randomInt() % 10 + 1;

    //     ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    //     assert(ib_trx != NULL);

    //     err = open_table(dbname, name, ib_trx, &crsr);
    //     assert(err == DB_SUCCESS);

    //     err = ib_cursor_lock(crsr, IB_LOCK_IX);
    //     assert(err == DB_SUCCESS);

    //     while (query_num)
    //     {
    //         query_num--;
    //         int op = rnd.randomInt() % 100;

    //         if (op < read_ratio)
    //         {
    //             // read random row
    //         }
    //         else
    //         {
    //             // update random row
    //         }
    //     }

    //     err = ib_cursor_close(crsr);
    //     assert(err == DB_SUCCESS);
    //     crsr = NULL;

    //     err = ib_trx_commit(ib_trx);
    //     assert(err == DB_SUCCESS);
    // }

    printf("Begin transaction\n");
    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    printf("Open cursor\n");
    err = open_table(dbname, name, ib_trx, &crsr);
    assert(err == DB_SUCCESS);

    printf("Lock table in IX\n");
    err = ib_cursor_lock(crsr, IB_LOCK_IX);
    assert(err == DB_SUCCESS);

    printf("Query table\n");
    err = do_query(crsr);
    assert(err == DB_SUCCESS);

    int pkey = 5;
    printf("Read tuple %d\n", pkey);
    err = update_tuple(crsr, pkey);
    assert(err == DB_SUCCESS);

    printf("Query table\n");
    err = do_query(crsr);
    assert(err == DB_SUCCESS);

    printf("Close cursor\n");
    err = ib_cursor_close(crsr);
    assert(err == DB_SUCCESS);
    crsr = NULL;

    printf("Commit transaction\n");
    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);

    return err;
}