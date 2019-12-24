#include "tpcc-helper.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>


std::vector<RandomGenerator> tpcc_rdm;

uint64_t Rand(uint64_t max, uint64_t thd_id) {
    assert(thd_id < tpcc_rdm.size());
    int64_t rint64 = 0;
    rint64 = tpcc_rdm[thd_id].randomInt();
    return rint64 % max;
}

uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id) {
    return x + Rand(y - x + 1, thd_id);
}

uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id) {
    static bool C_255_init = false;
    static bool C_1023_init = false;
    static bool C_8191_init = false;
    static uint64_t C_255, C_1023, C_8191;
    int C = 0;
    switch (A) {
    case 255:
        if (!C_255_init) {
            C_255 = (uint64_t)URand(0, 255, thd_id);
            C_255_init = true;
        }
        C = C_255;
        break;
    case 1023:
        if (!C_1023_init) {
            C_1023 = (uint64_t)URand(0, 1023, thd_id);
            C_1023_init = true;
        }
        C = C_1023;
        break;
    case 8191:
        if (!C_8191_init) {
            C_8191 = (uint64_t)URand(0, 8191, thd_id);
            C_8191_init = true;
        }
        C = C_8191;
        break;
    default:
        assert(false);
        exit(-1);
    }
    return (((URand(0, A, thd_id) | URand(x, y, thd_id)) + C) % (y - x + 1)) +
           x;
}

std::string MakeAlphaString(int min, int max, uint64_t thd_id) {
    const char char_list[] = {
        '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
        'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
        'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
        'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q',
        'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
    uint64_t cnt = URand(min, max, thd_id);
    std::string str;
    for (uint32_t i = 0; i < cnt; i++)
        str += char_list[URand(0L, 60L, thd_id)];
    return str;
}

std::string MakeNumberString(int min, int max, uint64_t thd_id) {
    uint64_t cnt = URand(min, max, thd_id);
    std::string str;
    for (uint32_t i = 0; i < cnt; i++) {
        uint64_t r = URand(0L, 9L, thd_id);
        str += (char)('0' + r);
    }
    return str;
}

// uint64_t distKey(uint64_t d_id, uint64_t d_w_id) {
//     return d_w_id * WorkloadConfig::g_dist_per_ware + d_id;
// }

// uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
//     return (distKey(c_d_id, c_w_id) * WorkloadConfig::g_cust_per_dist + c_id);
// }

// uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
//     return distKey(d_id, w_id) * WorkloadConfig::g_cust_per_dist + o_id;
// }

// uint64_t orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
//     return orderlineKey(w_id, d_id, o_id);
// }

// uint64_t custNPKey(const std::string &c_last, uint64_t c_d_id,
//                    uint64_t c_w_id) {
//     uint64_t key = 0;
//     char offset = 'A';
//     for (char i : c_last)
//         key = (key << 2) + (i - offset);
//     key = key << 3;
//     key += c_w_id * WorkloadConfig::g_dist_per_ware + c_d_id;
//     return key;
// }

// uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id) {
//     return s_w_id * WorkloadConfig::g_max_items + s_i_id;
// }

std::string Lastname(uint64_t num) {
    static const char *n[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                              "ESE", "ANTI",  "CALLY", "ATION", "EING"};
    std::string name;
    name += n[num / 100];
    name += n[(num / 10) % 10];
    name += n[num % 10];
    return name;
}