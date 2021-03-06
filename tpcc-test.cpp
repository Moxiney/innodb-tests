#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <thread>
#include <iostream>
#include <memory>
#include <getopt.h>

#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include "common.h"
#include "tpcc.h"
#include "tpcc-aux.h"
#include "tpcc-helper.h"

#define DATABASE "test"
#define TABLE "t"
#define INDEX "F0"

static void usage_exit()
{

	std::cout << "Command line options : tpcc_bench <options> \n";
	std::cout << "   -h --help              : Print help message \n";
	std::cout << "   -n --num               : Thread num\n";
	std::cout << "   -d --duration          : Duration time: (second)\n";
	std::cout << "   -w --warehouse         : number of warehouses(1-10)\n";
	exit(EXIT_FAILURE);
}

static struct option opts[] = {
	{"help", no_argument, NULL, 'h'},
	{"num", required_argument, NULL, 'n'},
	{"duration", required_argument, NULL, 'd'},
	{"warehouse", required_argument, NULL, 'w'},
	{"buff_size", required_argument, NULL, 'b'},
};

int main(int argc, char *argv[])
{
	// To do: assign values
	// init_table_size = 100000;
	num_wh = 3;
	int thread_num = 4;
	int duration = 10;
	int buff_size = 400;

	// Parse args
	while (1) {
        int idx = 0;
        int c = getopt_long(argc, argv, "h:n:d:w:b:", opts, &idx);

        if (c == -1)
            break;

        switch (c) {
        case 'n':
            thread_num = atoi(optarg);
            break;
        case 'd':
            duration = atoi(optarg);
            break;
        case 'w':
            num_wh = atoi(optarg);
            break;
		case 'b':
            buff_size = atoi(optarg);
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
	printf("warehouse number %d\t", num_wh);
	printf("thread number %d \t", thread_num);
	printf("duration %d\t", duration);
	printf("buff size %d\n", buff_size);
	




	ib_err_t err;
	ib_crsr_t crsr;
	ib_trx_t ib_trx;
	ib_u64_t version;

	// version = ib_api_version();
	// printf("API: %d.%d.%d\n",
	// 	   (int)(version >> 32),			/* Current version */
	// 	   (int)((version >> 16)) & 0xffff, /* Revisiion */
	// 	   (int)(version & 0xffff));		/* Age */

	tpcc_db_t tpcc_db = {
		"TPCC",
		{WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, ORDER, ORDER_LINE, ITEM, STOCK},
		num_wh};
	err = tpcc_db.init(buff_size);


	// multi_thread_query
	std::thread threads[thread_num];
	// int num[thread_num];
	cpuCycleTimer timers[thread_num];
	auto barrier = std::make_unique<Barrier>(thread_num + 1);

	auto rnd_size = thread_num > num_wh? thread_num: num_wh;
	tpcc_rdm.resize(rnd_size);

	for (int i = 0; i < thread_num; i++)
	{
		// num[i] = 0;
		threads[i] = std::thread(
			[&](int id) {
				// printf("thread %d start to run_txn\n", id);
				//err = ycsb_run_txn(DATABASE, TABLE, read_ratio, id, num[id], barrier.get());
				stick_this_thread_to_core(id);
				err = tpcc_run_txn(&tpcc_db, id, timers[id], barrier.get());
				// assert(err == DB_SUCCESS);
			},
			i);
	}

	barrier->wait();
	std::this_thread::sleep_for(std::chrono::seconds(duration));
	done = 1;

	// printf("done, wait for join\n");

	int res = 0;
	long cycle_total = 0;
	for (int i = 0; i < thread_num; i++)
	{
		threads[i].join();
		res += timers[i].get_count();
		cycle_total += timers[i].get_total();
	}
	

	err = tpcc_db.shutdown();

	printf("total res %d, tps %f\t", res, (double)res / duration);
	printf("avg latency %f\n\n", (double)cycle_total / res);

	return (EXIT_SUCCESS);
}
