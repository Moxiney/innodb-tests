#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <random>
#include "common.h"

extern std::vector<RandomGenerator> tpcc_rdm;

// return random data from [0, max-1]
uint64_t Rand(uint64_t max, uint64_t thd_id);

// random number from [x, y]
uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id);

// non-uniform random number
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id);

// random string with random length beteen min and max.
std::string MakeAlphaString(int min, int max, uint64_t thd_id);

std::string MakeNumberString(int min, int max, uint64_t thd_id);