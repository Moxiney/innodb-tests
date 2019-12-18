#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <thread>

#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include "common.h"
#include "ycsb.h"

#define DATABASE "test"
#define TABLE "t"
#define INDEX "F0"

int main(int argc, char *argv[])
{
	init_table_size = 100;
	int read_ratio = 50;
	int thread_num = 1;
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


	err = database_init(DATABASE);
	assert(err == DB_SUCCESS);

	err = ycsb_init(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	// multi_thread_query
	std::thread threads[thread_num];
	int num[thread_num];
	auto barrier = std::make_unique<Barrier>(thread_num + 1);

	for (int i = 0; i < thread_num; i++)
	{
		num[i] = 0;
		threads[i] = std::thread(
			[&](int id) {
				printf("thread %d start to run_txn\n", id);
				err = ycsb_run_txn(DATABASE, TABLE, read_ratio, id, num[id], barrier.get());
				assert(err == DB_SUCCESS);
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
	
	printf("Drop table\n");
	err = drop_table(DATABASE, TABLE);
	assert(err == DB_SUCCESS);

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	printf("total res %d, tps %f\n", res, (double)res/duration);

	return (EXIT_SUCCESS);
}
