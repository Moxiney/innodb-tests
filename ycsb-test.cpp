#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <thread>
#include <getopt.h>
#include <iostream>

#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include "common.h"
#include "ycsb.h"

#define DATABASE "test"
#define TABLE "t"
#define INDEX "F0"

static void usage_exit()
{
	std::cout << "Command line options : ycsb_bench <options> \n";
	std::cout << "   -h --help              : Print help message \n";
	std::cout << "   -n --num               : Thread num\n";
	std::cout << "   -d --duration          : Duration time: (second)\n";
	std::cout << "   -k --key_number        : Key Number(>0)\n";
	std::cout << "   -b --benchmark         : Benchmark: 0-5(ABCDEF)";
	exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"help", no_argument, NULL, 'h'},
    {"num", required_argument, NULL, 'n'},
    {"duration", required_argument, NULL, 'd'},
    {"key_number", required_argument, NULL, 'k'},
};

int main(int argc, char *argv[])
{
	init_table_size = 100000;
	int read_ratio = 50;
	int thread_num = 1;
	int duration = 10;

	// Parse args
	while (1)
	{
		int idx = 0;
		int c = getopt_long(argc, argv, "h:n:d:k:b:", opts, &idx);

		if (c == -1)
			break;

		switch (c)
		{
		case 'n':
			thread_num = atoi(optarg);
			break;
		case 'd':
			duration = atoi(optarg);
			break;
		case 'k':
			init_table_size = atoi(optarg);
			break;
		case 'b':
			//bench = (workload::YCSBWorkLoad)atoi(optarg);
			break;
		case 'h':
			usage_exit();
			break;
		default:
			fprintf(stderr, "\nUnknown option: -%c-\n", c);
			usage_exit();
			break;
		}
	}

	printf("key number %d\n", init_table_size);
	printf("thread number %d \n", thread_num);
	printf("duration %d\n", duration);

	

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

	printf("total res %d, tps %f\n", res, (double)res / duration);

	return (EXIT_SUCCESS);
}
