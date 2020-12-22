/**
 * @file ycsb-test.cpp
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2020-12-22
 *
 * @copyright Copyright (c) 2020
 *
 * Todo: add recover bench
 */

#include <assert.h>
#include <ctype.h>
#include <getopt.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <thread>
#include <time.h>

#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include "common.h"
#include "ycsb.h"

#define DATABASE "test"
#define TABLE "t"
#define INDEX "F0"

static void usage_exit() {
  std::cout << "Command line options : ycsb_bench <options> \n";
  std::cout << "   -h --help              : Print help message \n";
  std::cout << "   -n --num               : Thread num\n";
  std::cout << "   -d --duration          : Duration time: (second)\n";
  std::cout << "   -k --key_number        : Key Number(>0)\n";
  std::cout << "   -b --benchmark         : Benchmark: 0-5(ABCDEF)\n";
  std::cout << "   -R --recover       	  : Recover benchmark\n";
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"help", no_argument, NULL, 'h'},
    {"num", required_argument, NULL, 'n'},
    {"duration", required_argument, NULL, 'd'},
    {"key_number", required_argument, NULL, 'k'},
    {"buff_size", required_argument, NULL, 'b'},
    {"read_ratio", required_argument, NULL, 'r'},
    {"recover", no_argument, NULL, 'R'},
};

int main(int argc, char *argv[]) {
  init_table_size = 100000;
  int read_ratio = 50;
  int thread_num = 1;
  int duration = 10;
  int buff_size = 400;
  bool only_recover = false;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "h:n:d:k:b:r:R", opts, &idx);

    if (c == -1)
      break;

    switch (c) {
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
      buff_size = atoi(optarg);
      break;
    case 'r':
      read_ratio = atoi(optarg);
      break;
    case 'h':
      usage_exit();
      break;
    case 'R':
      only_recover = true;
      break;
    default:
      fprintf(stderr, "\nUnknown option: -%c-\n", c);
      usage_exit();
      break;
    }
  }

  printf("key number %d\t", init_table_size);
  printf("thread number %d \t", thread_num);
  printf("duration %d\t", duration);
  printf("read ratio %d\t", read_ratio);
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
  auto start = std::chrono::system_clock::now();

  err = database_init(DATABASE, buff_size);
  assert(err == DB_SUCCESS);

  auto end = std::chrono::system_clock::now();

  if (only_recover) {
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "total reboot time " << elapsed.count() << std::endl;
  }

  err = ycsb_init(DATABASE, TABLE);
  assert(err == DB_SUCCESS);

  // multi_thread_query
  std::thread threads[thread_num];
  // int num[thread_num];
  cpuCycleTimer timers[thread_num];
  auto barrier = std::make_unique<Barrier>(thread_num + 1);

  for (int i = 0; i < thread_num; i++) {
    // num[i] = 0;
    threads[i] = std::thread(
        [&](int id) {
          // printf("thread %d start to run_txn\n", id);
          stick_this_thread_to_core(id);
          err = ycsb_run_txn(DATABASE, TABLE, read_ratio, id, timers[id],
                             barrier.get());
          assert(err == DB_SUCCESS);
        },
        i);
  }

  barrier->wait();
  std::this_thread::sleep_for(std::chrono::seconds(duration));
  done = 1;

  // printf("done, wait for join\n");

  int res = 0;
  long cycle_total = 0;

  /* Crash */
  if (only_recover) {
    res += timers[i].get_count();
    cycle_total += timers[i].get_total();
    printf("total res %d, tps %f\t", res, (double)res / duration);
    printf("avg latency %f\n\n", (double)cycle_total / res);
    printf("System crashed.\n");
    return 1;
  }

  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
    res += timers[i].get_count();
    cycle_total += timers[i].get_total();
  }

  // printf("Drop table\n");
  // err = drop_table(DATABASE, TABLE);
  // assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  printf("total res %d, tps %f\t", res, (double)res / duration);
  printf("avg latency %f\n\n", (double)cycle_total / res);

  return (EXIT_SUCCESS);
}
