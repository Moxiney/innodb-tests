#!/bin/bash

# for bench in 10 50 90
# do
#     for num in 1 2 4 6 8 10 12 14 16 18
#     do
#         make clean-data
#         ./ycsb-test -n ${num} -d 10 -k 50000000 -r ${bench} >> ./ycsb.res
#     done
# done

# for warehourse in 1 4
# do
#     for num in 1 2 4 6 8 10 12 14 16 18
#     do
#         make clean-data
#         ./tpcc-test -n ${num} -d 10 -w ${warehourse} >> ./tpcc.res
#     done
# done

# for num in 1 2 4 8 10 12 14 16 18
# do
#     make clean-data
#     ../build/bin/tpcc_bench -n ${num} -d 10 -w ${num} >> ./tpcc.res
# done

for warehourse in 1 4
do
    for num in 1 2 4 6 8 
    do
        make clean-data
        ./tpcc-test -n ${num} -d 10 -w ${warehourse} >> ./tpcc.res
    done
done