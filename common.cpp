#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "common.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <getopt.h>

static const char log_group_home_dir[] = "log";
static const char data_file_path[] = "ibdata1:128M:autoextend";

ib_err_t
create_database(
    /*============*/
    const char *name)
{
    ib_bool_t err;

    err = ib_database_create(name);
    assert(err == IB_TRUE);

    return (DB_SUCCESS);
}

static void
create_directory(
    /*=============*/
    const char *path)
{
    int ret;
    /* Try and create the log sub-directory */
    ret = mkdir(path, S_IRWXU);

    /* Note: This doesn't catch all errors. EEXIST can also refer to
	dangling symlinks. */
    if (ret == -1 && errno != EEXIST)
    {
        perror(path);
        exit(EXIT_FAILURE);
    }
}

ib_err_t test_configure(void)
/*================*/
{
    ib_err_t err;

    create_directory(log_group_home_dir);

    err = ib_cfg_set_text("flush_method", "O_DIRECT");
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("log_files_in_group", 2);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("log_file_size", 32 * 1024 * 1024);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("log_buffer_size", 24 * 16384);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("buffer_pool_size", 5 * 1024 * 1024);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("additional_mem_pool_size", 4 * 1024 * 1024);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("flush_log_at_trx_commit", 1);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("file_io_threads", 4);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("lock_wait_timeout", 10);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_int("open_files", 300);
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_bool_on("doublewrite");
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_bool_on("checksums");
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_bool_on("rollback_on_timeout");
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_bool_on("print_verbose_log");
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_bool_on("file_per_table");
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_text("data_home_dir", "./");
    assert(err == DB_SUCCESS);

    err = ib_cfg_set_text("log_group_home_dir", log_group_home_dir);

    if (err != DB_SUCCESS)
    {
        fprintf(stderr,
                "syntax error in log_group_home_dir, or a "
                "wrong number of mirrored log groups\n");
        exit(1);
    }

    err = ib_cfg_set_text("data_file_path", data_file_path);

    if (err != DB_SUCCESS)
    {
        fprintf(stderr,
                "InnoDB: syntax error in data_file_path\n");
        exit(1);
    }

    return err;
}

/*********************************************************************
Open a table and return a cursor for the table. */
ib_err_t
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

ib_err_t database_init(
    const char *dbname)
{
    ib_err_t err;
    err = ib_init();
	assert(err == DB_SUCCESS);

	err = test_configure();
	assert(err == DB_SUCCESS);

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	err = create_database(dbname);
	assert(err == DB_SUCCESS);

    return err;
}

ib_err_t
drop_table(
    /*=======*/
    const char *dbname, /*!< in: database name */
    const char *name)   /*!< in: table to drop */
{
    ib_err_t err;
    ib_trx_t ib_trx;
    char table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
    sprintf(table_name, "%s/%s", dbname, name);
#else
    snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    err = ib_schema_lock_exclusive(ib_trx);
    assert(err == DB_SUCCESS);

    err = ib_table_drop(ib_trx, table_name);
    assert(err == DB_SUCCESS);

    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);

    return (err);
}

void print_char_array(
    /*=============*/
    FILE *stream,      /*!< in: stream to print to */
    const char *array, /*!< in: char array */
    int len)           /*!< in: length of data */
{
    int i;
    const char *ptr = array;

    // fprintf(stream, "%s", ptr);

    for (i = 0; i < len; ++i)
    {
        fprintf(stream, "%c", *(ptr + i));
    }
    fprintf(stream, "\t");
}

/*********************************************************************
Print all columns in a tuple. */

void print_int_col(
    /*==========*/
    FILE *stream,
    const ib_tpl_t tpl,
    int i,
    ib_col_meta_t *col_meta)
{
    ib_err_t err = DB_SUCCESS;

    switch (col_meta->type_len)
    {
    case 1:
    {
        if (col_meta->attr & IB_COL_UNSIGNED)
        {
            ib_u8_t u8;

            err = ib_tuple_read_u8(tpl, i, &u8);
            fprintf(stream, "%u", u8);
        }
        else
        {
            ib_i8_t i8;

            err = ib_tuple_read_i8(tpl, i, &i8);
            fprintf(stream, "%d", i8);
        }
        break;
    }
    case 2:
    {
        if (col_meta->attr & IB_COL_UNSIGNED)
        {
            ib_u16_t u16;

            err = ib_tuple_read_u16(tpl, i, &u16);
            fprintf(stream, "%u", u16);
        }
        else
        {
            ib_i16_t i16;

            err = ib_tuple_read_i16(tpl, i, &i16);
            fprintf(stream, "%d", i16);
        }
        break;
    }
    case 4:
    {
        if (col_meta->attr & IB_COL_UNSIGNED)
        {
            ib_u32_t u32;

            err = ib_tuple_read_u32(tpl, i, &u32);
            fprintf(stream, "%u", u32);
        }
        else
        {
            ib_i32_t i32;

            err = ib_tuple_read_i32(tpl, i, &i32);
            fprintf(stream, "%d", i32);
        }
        break;
    }
    case 8:
    {
        if (col_meta->attr & IB_COL_UNSIGNED)
        {
            ib_u64_t u64;

            err = ib_tuple_read_u64(tpl, i, &u64);
            fprintf(stream, "%llu", (unsigned long long)u64);
        }
        else
        {
            ib_i64_t i64;

            err = ib_tuple_read_i64(tpl, i, &i64);
            fprintf(stream, "%lld", (long long)i64);
        }
        break;
    }
    default:
        assert(0);
        break;
    }
    assert(err == DB_SUCCESS);
}

void print_tuple(
    /*========*/
    FILE *stream,
    const ib_tpl_t tpl)
{
    int i;
    int n_cols = ib_tuple_get_n_cols(tpl);

    for (i = 0; i < n_cols; ++i)
    {
        ib_ulint_t data_len;
        ib_col_meta_t col_meta;

        data_len = ib_col_get_meta(tpl, i, &col_meta);

        /* Skip system columns. */
        if (col_meta.type == IB_SYS)
        {
            continue;
            /* Nothing to print. */
        }
        else if (data_len == IB_SQL_NULL)
        {
            fprintf(stream, "|");
            continue;
        }
        else
        {
            switch (col_meta.type)
            {
            case IB_INT:
            {
                print_int_col(stream, tpl, i, &col_meta);
                break;
            }
            case IB_FLOAT:
            {
                float v;
                ib_err_t err;

                err = ib_tuple_read_float(tpl, i, &v);
                assert(err == DB_SUCCESS);
                fprintf(stream, "%f", v);
                break;
            }
            case IB_DOUBLE:
            {
                double v;
                ib_err_t err;

                err = ib_tuple_read_double(tpl, i, &v);
                assert(err == DB_SUCCESS);
                fprintf(stream, "%lf", v);
                break;
            }
            case IB_CHAR:
            case IB_BLOB:
            case IB_DECIMAL:
            case IB_VARCHAR:
            {
                //const char*	ptr;

                auto ptr = ib_col_get_value(tpl, i);
                auto cptr = static_cast<const char *>(ptr);
                fprintf(stream, "%d:", (int)data_len);
                print_char_array(stream, cptr, (int)data_len);
                break;
            }
            default:
                assert(IB_FALSE);
                break;
            }
        }
        fprintf(stream, "|");
    }
    fprintf(stream, "\n");
}

/*********************************************************************
SELECT * FROM T; */
ib_err_t
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