#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>




#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include "common.h"
#include "ycsb.h"


#define DATABASE "test"
#define TABLE "t"

typedef struct row_t {
	char		c1[32];
	char		c2[32];
	ib_u32_t	c3;
} row_t;

static row_t in_rows[] = {
	{"a",	"t",	1},
	{"b",	"u",	2},
	{"c",	"b",	3},
	{"d",	"n",	4},
	{"e",	"s",	5},
	{"e",	"j",	6},
	{"d",	"f",	7},
	{"c",	"n",	8},
	{"b",	"z",	9},
	{"a",	"i",	10},
	{"",	"",	0}};

#define COL_LEN(n)	(sizeof(((row_t*)0)->n))



/**********************************************************************
Print character array of give size or upto 256 chars */

// void
// print_char_array(
// /*=============*/
// 	FILE*		stream,	/*!< in: stream to print to */
// 	const char*	array,	/*!< in: char array */
// 	int		len)	/*!< in: length of data */
// {
// 	int		i;
// 	const char*	ptr = array;

// 	for (i = 0; i < len; ++i) {
// 		fprintf(stream, "%c", *(ptr + i));
// 	}
// }

// /*********************************************************************
// Print all columns in a tuple. */

// void
// print_int_col(
// /*==========*/
// 	FILE*		stream,
// 	const ib_tpl_t	tpl,
// 	int		i,
// 	ib_col_meta_t*	col_meta)
// {
// 	ib_err_t	err = DB_SUCCESS;

// 	switch (col_meta->type_len) {
// 	case 1: {
// 		if (col_meta->attr & IB_COL_UNSIGNED) {
// 			ib_u8_t		u8;

// 			err = ib_tuple_read_u8(tpl, i, &u8);
// 			fprintf(stream, "%u", u8);
// 		} else {
// 			ib_i8_t		i8;

// 			err = ib_tuple_read_i8(tpl, i, &i8);
// 			fprintf(stream, "%d", i8);
// 		}
// 		break;
// 	}
// 	case 2: {
// 		if (col_meta->attr & IB_COL_UNSIGNED) {
// 			ib_u16_t	u16;

// 			err = ib_tuple_read_u16(tpl, i, &u16);
// 			fprintf(stream, "%u", u16);
// 		} else {
// 			ib_i16_t	i16;

// 			err = ib_tuple_read_i16(tpl, i, &i16);
// 			fprintf(stream, "%d", i16);
// 		}
// 		break;
// 	}
// 	case 4: {
// 		if (col_meta->attr & IB_COL_UNSIGNED) {
// 			ib_u32_t	u32;

// 			err = ib_tuple_read_u32(tpl, i, &u32);
// 			fprintf(stream, "%u", u32);
// 		} else {
// 			ib_i32_t	i32;

// 			err = ib_tuple_read_i32(tpl, i, &i32);
// 			fprintf(stream, "%d", i32);
// 		}
// 		break;
// 	}
// 	case 8: {
// 		if (col_meta->attr & IB_COL_UNSIGNED) {
// 			ib_u64_t	u64;

// 			err = ib_tuple_read_u64(tpl, i, &u64);
// 			fprintf(stream, "%llu", (unsigned long long) u64);
// 		} else {
// 			ib_i64_t	i64;

// 			err = ib_tuple_read_i64(tpl, i, &i64);
// 			fprintf(stream, "%lld",  (long long) i64);
// 		}
// 		break;
// 	}
// 	default:
// 		assert(0);
// 		break;
// 	}
// 	assert(err == DB_SUCCESS);
// }

// void
// print_tuple(
// /*========*/
// 	FILE*		stream,
// 	const ib_tpl_t	tpl)
// {
// 	int		i;
// 	int		n_cols = ib_tuple_get_n_cols(tpl);

// 	for (i = 0; i < n_cols; ++i) {
// 		ib_ulint_t	data_len;
// 		ib_col_meta_t	col_meta;

// 		data_len = ib_col_get_meta(tpl, i, &col_meta);

// 		/* Skip system columns. */
// 		if (col_meta.type == IB_SYS) {
// 			continue;
// 		/* Nothing to print. */
// 		} else if (data_len == IB_SQL_NULL) {
// 			fprintf(stream, "|");
// 			continue;
// 		} else {
// 			switch (col_meta.type) {
// 			case IB_INT: {
// 				print_int_col(stream, tpl, i, &col_meta);
// 				break;
// 			}
// 			case IB_FLOAT: {
// 				float		v;
// 				ib_err_t	err;

// 				err = ib_tuple_read_float(tpl, i, &v);
// 				assert(err == DB_SUCCESS);
// 				fprintf(stream, "%f", v);
// 				break;
// 			}
// 			case IB_DOUBLE: {
// 				double		v;
// 				ib_err_t	err;

// 				err = ib_tuple_read_double(tpl, i, &v);
// 				assert(err == DB_SUCCESS);
// 				fprintf(stream, "%lf", v);
// 				break;
// 			}
// 			case IB_CHAR:
// 			case IB_BLOB:
// 			case IB_DECIMAL:
// 			case IB_VARCHAR: {
// 				const char*	ptr;

// 				auto ptr = ib_col_get_value(tpl, i);
// 				auto cptr = static_cast< const char* >( ptr );
// 				fprintf(stream, "%d:", (int) data_len);
// 				print_char_array(stream, cptr, (int) data_len);
// 				break;
// 			}
// 			default:
// 				assert(IB_FALSE);
// 				break;
// 			}
// 		}
// 		fprintf(stream, "|");
// 	}
// 	fprintf(stream, "\n");
// }

// /*********************************************************************
// SELECT * FROM T; */
// static
// ib_err_t
// do_query(
// /*=====*/
// 	ib_crsr_t	crsr)
// {
// 	ib_err_t	err;
// 	ib_tpl_t	tpl;

// 	tpl = ib_clust_read_tuple_create(crsr);
// 	assert(tpl != NULL);

// 	err = ib_cursor_first(crsr);
// 	assert(err == DB_SUCCESS);

// 	while (err == DB_SUCCESS) {
// 		err = ib_cursor_read_row(crsr, tpl);

// 		assert(err == DB_SUCCESS
// 		       || err == DB_END_OF_INDEX
// 		       || err == DB_RECORD_NOT_FOUND);

// 		if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
// 			break;
// 		}

// 		print_tuple(stdout, tpl);

// 		err = ib_cursor_next(crsr);

// 		assert(err == DB_SUCCESS
// 		       || err == DB_END_OF_INDEX
// 		       || err == DB_RECORD_NOT_FOUND);

// 		tpl = ib_tuple_clear(tpl);
// 		assert(tpl != NULL);
// 	}

// 	if (tpl != NULL) {
// 		ib_tuple_delete(tpl);
// 	}

// 	if (err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX) {
// 		err = DB_SUCCESS;
// 	}
// 	return(err);
// }

static
ib_err_t
create_database(
/*============*/
	const char*	name)
{
	ib_bool_t	err;

	err = ib_database_create(name);
	assert(err == IB_TRUE);

	return(DB_SUCCESS);
}

static
ib_err_t
create_table(
/*=========*/
	const char*	dbname,			/*!< in: database name */
	const char*	name)			/*!< in: table name */
{
	ib_trx_t	ib_trx;
	ib_id_t		table_id = 0;
	ib_err_t	err = DB_SUCCESS;
	ib_tbl_sch_t	ib_tbl_sch = NULL;
	ib_idx_sch_t	ib_idx_sch = NULL;
	char		table_name[IB_MAX_TABLE_NAME_LEN];


	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
	printf("Creating %s\n", table_name);

	/* Pass a table page size of 0, ie., use default page size. */
	err = ib_table_schema_create(
		table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c1",
		IB_VARCHAR, IB_COL_NONE, 0, COL_LEN(c1)-1);

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c2",
		IB_VARCHAR, IB_COL_NONE, 0, COL_LEN(c2)-1);
	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_col(
		ib_tbl_sch, "c3",
		IB_INT, IB_COL_UNSIGNED, 0, COL_LEN(c3));

	assert(err == DB_SUCCESS);

	err = ib_table_schema_add_index(ib_tbl_sch, "c1_c2", &ib_idx_sch);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "c1", 0);
	assert(err == DB_SUCCESS);

	/* Set prefix length to 0. */
	err = ib_index_schema_add_col( ib_idx_sch, "c2", 0);
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

	if (ib_tbl_sch != NULL) {
		ib_table_schema_delete(ib_tbl_sch);
	}

	return(err);
}

/*********************************************************************
Open a table and return a cursor for the table. */
static
ib_err_t
open_table(
/*=======*/
	const char*	dbname,		/*!< in: database name */
	const char*	name,		/*!< in: table name */
	ib_trx_t	ib_trx,		/*!< in: transaction */
	ib_crsr_t*	crsr)		/*!< out: innodb cursor */
{
	ib_err_t	err = DB_SUCCESS;
	char		table_name[IB_MAX_TABLE_NAME_LEN];


	snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

	err = ib_cursor_open_table(table_name, ib_trx, crsr);
	assert(err == DB_SUCCESS);

	return(err);
}

/*********************************************************************
INSERT INTO T VALUE('c1', 'c2', c3); */
static
ib_err_t
insert_rows(
/*========*/
	ib_crsr_t	crsr)		/*!< in, out: cursor to use for write */
{
	row_t*		row;
	ib_tpl_t	tpl = NULL;
	ib_err_t	err = DB_ERROR;

	tpl = ib_clust_read_tuple_create(crsr);
	assert(tpl != NULL);

	for (row = in_rows; *row->c1; ++row) {
		err = ib_col_set_value(tpl, 0, row->c1, strlen(row->c1));
		assert(err == DB_SUCCESS);

		err = ib_col_set_value(tpl, 1, row->c2, strlen(row->c2));
		assert(err == DB_SUCCESS);

		err = ib_col_set_value(tpl, 2, &row->c3, sizeof(row->c3));
		assert(err == DB_SUCCESS);

		err = ib_cursor_insert_row(crsr, tpl);
		assert(err == DB_SUCCESS);

		tpl = ib_tuple_clear(tpl);
		assert(tpl != NULL);
	}

	if (tpl != NULL) {
		ib_tuple_delete(tpl);
	}

	return(err);
}

/*********************************************************************
UPDATE T SET c3 = c3 + 100 WHERE c1 = 'a'; */
static
ib_err_t
update_a_row(
/*=========*/
	ib_crsr_t	crsr)
{
	ib_err_t	err;
	int		res = ~0;
	ib_tpl_t	key_tpl;
	ib_tpl_t	old_tpl = NULL;
	ib_tpl_t	new_tpl = NULL;

	/* Create a tuple for searching an index. */
	key_tpl = ib_sec_search_tuple_create(crsr);
	assert(key_tpl != NULL);

	/* Set the value to look for. */
	err = ib_col_set_value(key_tpl, 0, "a", 1);
	assert(err == DB_SUCCESS);

	/* Search for the key using the cluster index (PK) */
	err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
	assert(err == DB_SUCCESS);
	/* Must be positioned on a record that's greater than search key. */
	assert(res == -1);

	if (key_tpl != NULL) {
		ib_tuple_delete(key_tpl);
	}

	/* Create the tuple instance that we will use to update the
	table. old_tpl is used for reading the existing row and
	new_tpl will contain the update row data. */

	old_tpl = ib_clust_read_tuple_create(crsr);
	assert(old_tpl != NULL);

	new_tpl = ib_clust_read_tuple_create(crsr);
	assert(new_tpl != NULL);

	/* Iterate over the records while the c1 column matches "a". */
	while (err == DB_SUCCESS) {
		const char*	c1;
		ib_u32_t	c3;
		ib_ulint_t	c1_len;
		ib_ulint_t	data_len;
		ib_col_meta_t	col_meta;

		err = ib_cursor_read_row(crsr, old_tpl);
		assert(err == DB_SUCCESS);

		/* Get the c1 column value. */
		auto ptr = ib_col_get_value(old_tpl, 0);
		c1 = static_cast<const char *>(ptr);
		c1_len = ib_col_get_meta(old_tpl, 0, &col_meta);

		/* There are no SQL_NULL values in our test data. */
		assert(c1 != NULL);

		/* Only update c1 values that are == "a". */
		if (strncmp(c1, "a", 1) != 0) {
			break;
		}

		/* Copy the old contents to the new tuple. */
		err = ib_tuple_copy(new_tpl, old_tpl);

		/* Update the c3 column in the new tuple. */
		data_len = ib_col_get_meta(old_tpl, 2, &col_meta);
		assert(data_len != IB_SQL_NULL);
		err = ib_tuple_read_u32(old_tpl, 2, &c3);
		assert(err == DB_SUCCESS);
		c3 += 100;

		/* Set the updated value in the new tuple. */
		err = ib_tuple_write_u32(new_tpl, 2, c3);
		assert(err == DB_SUCCESS);

		err = ib_cursor_update_row(crsr, old_tpl, new_tpl);
		assert(err == DB_SUCCESS);

		/* Move to the next record to update. */
		err = ib_cursor_next(crsr);
		/* Since we are searching for "a" it must always succeed. */
		assert(err == DB_SUCCESS);

		/* Reset the old and new tuple instances. */
		old_tpl = ib_tuple_clear(old_tpl);
		assert(old_tpl != NULL);

		new_tpl = ib_tuple_clear(new_tpl);
		assert(new_tpl != NULL);
	}

	if (old_tpl != NULL) {
		ib_tuple_delete(old_tpl);
	}
	if (new_tpl != NULL) {
		ib_tuple_delete(new_tpl);
	}

	return(err);
}

/*********************************************************************
DELETE RFOM T WHERE c1 = 'b' and c2 = 'z'; */
static
ib_err_t
delete_a_row(
/*=========*/
	ib_crsr_t	crsr)
{
	ib_err_t	err;
	int		res = ~0;
	ib_tpl_t	key_tpl;

	/* Create a tuple for searching an index. */
	key_tpl = ib_sec_search_tuple_create(crsr);
	assert(key_tpl != NULL);

	/* Set the value to delete. */
	err = ib_col_set_value(key_tpl, 0, "b", 1);
	assert(err == DB_SUCCESS);
	err = ib_col_set_value(key_tpl, 1, "z", 1);
	assert(err == DB_SUCCESS);

	/* Search for the key using the cluster index (PK) */
	err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
	assert(err == DB_SUCCESS);
	/* Must be positioned on the record to delete, since
	we've specified an exact prefix match. */
	assert(res == 0);

	if (key_tpl != NULL) {
		ib_tuple_delete(key_tpl);
	}

	/* InnoDB handles the updating of all secondary indexes. */
	err = ib_cursor_delete_row(crsr);
	assert(err == DB_SUCCESS);

	return(err);
}

int main()
{
	ib_err_t err;
	ib_crsr_t crsr;
	ib_trx_t ib_trx;
	ib_u64_t version;


	version = ib_api_version();
	printf("API: %d.%d.%d\n",
		   (int)(version >> 32),			/* Current version */
		   (int)((version >> 16)) & 0xffff, /* Revisiion */
		   (int)(version & 0xffff));		/* Age */

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	err = create_database(DATABASE);
	assert(err == DB_SUCCESS);

    err = ycsb_init(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = ycsb_run_txn(DATABASE, TABLE);
	assert(err == DB_SUCCESS);
	
	// single_thread_query

	// printf("Create table\n");
	// err = create_table(DATABASE, TABLE);
	// assert(err == DB_SUCCESS);

	// printf("Begin transaction\n");
	// ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	// assert(ib_trx != NULL);

	// printf("Open cursor\n");
	// err = open_table(DATABASE, TABLE, ib_trx, &crsr);
	// assert(err == DB_SUCCESS);

	// printf("Lock table in IX\n");
	// err = ib_cursor_lock(crsr, IB_LOCK_IX);
	// assert(err == DB_SUCCESS);

	// printf("Insert rows\n");
	// err = insert_rows(crsr);
	// assert(err == DB_SUCCESS);

	// printf("Query table\n");
	// err = do_query(crsr);
	// assert(err == DB_SUCCESS);

	// printf("Update a row\n");
	// err = update_a_row(crsr);
	// assert(err == DB_SUCCESS);

	// printf("Query table\n");
	// err = do_query(crsr);
	// assert(err == DB_SUCCESS);

	// printf("Delete a row\n");
	// err = delete_a_row(crsr);
	// assert(err == DB_SUCCESS);
	
	// printf("Query table\n");
	// err = do_query(crsr);
	// assert(err == DB_SUCCESS);

	// printf("Close cursor\n");
	// err = ib_cursor_close(crsr);
	// assert(err == DB_SUCCESS);
	// crsr = NULL;

	// printf("Commit transaction\n");
	// err = ib_trx_commit(ib_trx);
	// assert(err == DB_SUCCESS);

	printf("Drop table\n");
	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);
	
	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	return (EXIT_SUCCESS);
}
