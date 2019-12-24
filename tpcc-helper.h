#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <random>
#include "common.h"

extern std::vector<RandomGenerator> tpcc_rdm;

uint64_t distKey(uint64_t d_id, uint64_t d_w_id);

uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id);

uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);

uint64_t orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);

// non-primary key
uint64_t custNPKey(const std::string &c_last, uint64_t c_d_id, uint64_t c_w_id);

uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id);

std::string Lastname(uint64_t num);

// return random data from [0, max-1]
uint64_t Rand(uint64_t max, uint64_t thd_id);

// random number from [x, y]
uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id);

// non-uniform random number
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id);

// random string with random length beteen min and max.
std::string MakeAlphaString(int min, int max, uint64_t thd_id);

std::string MakeNumberString(int min, int max, uint64_t thd_id);