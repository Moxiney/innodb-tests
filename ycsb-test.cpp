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
	init_table_size = 50;
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

	err = ycsb_run_txn(DATABASE, TABLE, read_ratio);
	assert(err == DB_SUCCESS);
	

	printf("Drop table\n");
	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);
	
	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	return (EXIT_SUCCESS);
}
