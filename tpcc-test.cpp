#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <thread>
#include <iostream>
#include <memory>

#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include "common.h"
#include "tpcc.h"
#include "tpcc-aux.h"
#include "tpcc-helper.h"

#define DATABASE "test"
#define TABLE "t"
#define INDEX "F0"

int main(int argc, char *argv[])
{
	// To do: assign values
    // init_table_size = 100000;
	num_wh = 3;
	int read_ratio = 50;
	int thread_num = 4;
	int duration = 10;

	// Parse args



	ib_err_t err;
	ib_crsr_t crsr;
	ib_trx_t ib_trx;
	ib_u64_t version;

	version = ib_api_version();
	printf("API: %d.%d.%d\n",
		   (int)(version >> 32),			/* Current version */
		   (int)((version >> 16)) & 0xffff, /* Revisiion */
		   (int)(version & 0xffff));		/* Age */

    tpcc_db_t tpcc_db = {
        "TPCC",
        {WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, ORDER, ORDER_LINE, ITEM, STOCK},
		num_wh
    };
    err = tpcc_db.init();

	// int num = 0;
	// auto barrier = std::make_unique<Barrier>(thread_num + 1);
	// for (int i = 0; i < 5; i++) {
	// 	ASSERT(tpcc_run_txn(&tpcc_db, 0, num, barrier.get()));
	// }

	// multi_thread_query
	std::thread threads[thread_num];
	int num[thread_num];
	auto barrier = std::make_unique<Barrier>(thread_num + 1);

	tpcc_rdm.resize(10);

	for (int i = 0; i < thread_num; i++)
	{
		num[i] = 0;
		threads[i] = std::thread(
			[&](int id) {
				printf("thread %d start to run_txn\n", id);
				//err = ycsb_run_txn(DATABASE, TABLE, read_ratio, id, num[id], barrier.get());
				err = tpcc_run_txn(&tpcc_db, id, num[id], barrier.get());
                // assert(err == DB_SUCCESS);
			},
			i);
	}

	barrier->wait();
    std::this_thread::sleep_for(std::chrono::seconds(duration));
    done = 1;
	
	printf("done, wait for join\n");

	int res = 0;
	for (int i = 0; i < thread_num; i++)
	{
		threads[i].join();
		res += num[i];
	}
	
	// printf("Drop table\n");
	// err = drop_table(DATABASE, TABLE);
	// assert(err == DB_SUCCESS);

	err  = tpcc_db.shutdown();

	printf("total res %d, tps %f\n", res, (double)res/duration);

	return (EXIT_SUCCESS);
}
