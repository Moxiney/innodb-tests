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
#define INDEX "F0"

int main()
{
	init_table_size = 10;
	int read_ratio = 50;
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

	// single_thread_query

	// err = ycsb_run_txn(DATABASE, TABLE, read_ratio);
	// assert(err == DB_SUCCESS);

	ib_trx_t trx_1, trx_2;
	ib_crsr_t crsr_1, crsr_2;

	trx_1 = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	trx_2 = ib_trx_begin(IB_TRX_REPEATABLE_READ);
	assert(ib_trx != NULL);

	err = open_table(DATABASE, TABLE, trx_1, &crsr_1);
	assert(err == DB_SUCCESS);

	err = open_table(DATABASE, TABLE, trx_2, &crsr_2);
	assert(err == DB_SUCCESS);

	err = ib_cursor_lock(crsr_1, IB_LOCK_IX);
	assert(err == DB_SUCCESS);

	err = ib_cursor_lock(crsr_2, IB_LOCK_IX);
	assert(err == DB_SUCCESS);

	printf("\n");
	err = update_tuple(crsr_2, 6);
	assert(err == DB_SUCCESS);
	err = read_tuple(crsr_2, 6);
	assert(err == DB_SUCCESS);

	err = update_tuple(crsr_1, 5);
	assert(err == DB_SUCCESS);

	err = read_tuple(crsr_1, 5);
	assert(err == DB_SUCCESS);

	err = update_tuple(crsr_2, 5);
	if (err != DB_SUCCESS)
	{
		assert(ib_trx_state(trx_2) != IB_TRX_ACTIVE);

		err = ib_cursor_close(crsr_2);
		assert(err == DB_SUCCESS);
		crsr_2 = NULL;

		err = ib_trx_release(trx_2);
		assert(err == DB_SUCCESS);
		printf("trx2 rolled back\n");
	}
	else
	{
		err = ib_cursor_close(crsr_2);
		assert(err == DB_SUCCESS);
		crsr_2 = NULL;
		err = ib_trx_commit(trx_2);
		assert(err == DB_SUCCESS);
		printf("trx2 committed back\n");
	}
	assert(err == DB_SUCCESS);

	err = ib_cursor_close(crsr_1);
	assert(err == DB_SUCCESS);
	crsr_1 = NULL;
	err = ib_trx_commit(trx_1);
	assert(err == DB_SUCCESS);
	printf("trx1 committed back\n");


	err = ycsb_display(DATABASE, TABLE);
	assert(err == DB_SUCCESS);


	printf("Drop table\n");
	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	return (EXIT_SUCCESS);
}
