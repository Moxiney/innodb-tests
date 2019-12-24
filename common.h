#pragma once

#include "/usr/local/include/embedded_innodb-1.0/innodb.h"
#include <cstdlib>
#include <cstring>
#include <atomic>

static void
create_directory(
    /*=============*/
    const char *path);

ib_err_t test_configure(void);

ib_err_t
create_database(
    /*=======*/
    const char *name);  /*!< in: table to drop */

/*********************************************************************
Open a table and return a cursor for the table. */
ib_err_t
open_table(
    /*=======*/
    const char *dbname, /*!< in: database name */
    const char *name,   /*!< in: table name */
    ib_trx_t ib_trx,    /*!< in: transaction */
    ib_crsr_t *crsr);    /*!< out: innodb cursor */


ib_err_t database_init(const char *dbname);

ib_err_t
drop_table(
    /*=======*/
    const char *dbname, /*!< in: database name */
    const char *name);  /*!< in: table to drop */

void print_char_array(
    /*=============*/
    FILE *stream,      /*!< in: stream to print to */
    const char *array, /*!< in: char array */
    int len);          /*!< in: length of data */

void print_int_col(
    /*==========*/
    FILE *stream,
    const ib_tpl_t tpl,
    int i,
    ib_col_meta_t *col_meta);

void print_tuple(
    /*========*/
    FILE *stream,
    const ib_tpl_t tpl);

ib_err_t
do_query(
    /*=====*/
    ib_crsr_t crsr);

class RandomGenerator
{
    unsigned short seed[3];
    unsigned short seed2[3];
    unsigned short inital[3];
    unsigned short inital2[3];

public:
    RandomGenerator()
    {
        for (int i = 0; i < 3; i++)
        {
            inital[i] = seed[i] = rand();
            inital2[i] = seed2[i] = rand();
        }
    }
    int randomInt() { return nrand48(seed) ^ nrand48(seed2); }
    double randomDouble() { return erand48(seed) * erand48(seed2); }
    void setSeed(unsigned short newseed[3])
    {
        memcpy(seed, newseed, sizeof(unsigned short) * 3);
    }
    void reset()
    {
        memcpy(seed, inital, sizeof(unsigned short) * 3);
        memcpy(seed2, inital2, sizeof(unsigned short) * 3);
    }
    long long Next() { return randomInt(); }
} __attribute__((aligned(64)));


class Barrier{
        int threads_num;
        std::atomic<int> now_threads;
    public:
        Barrier(int threads_num_): threads_num(threads_num_){
            now_threads.store(0);
        }
        void wait(){
            now_threads.fetch_add(1);
            while(now_threads.load() != threads_num) {}
        }
    };
