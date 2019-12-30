#!/bin/bash

#data size 5GB
for buffsize in 700 6000
do
    for bench in 10 50 90
    do
        for num in 1 2 4 6 8 10 12 14 16 18
        do
            make clean-data
            ./ycsb-test -n ${num} -d 10 -k 50000000 -r ${bench} -b ${buffsize} >> ./ycsb.res
        done
    done
done

#data size per wh may be 80MB
for buffsize in 10 100
do
    for warehourse in 1 4
    do
        for num in 1 2 4 6 8 10 12 14 16 18
        do
            make clean-data
            ./tpcc-test -n ${num} -d 10 -w ${warehourse} -b ${${buffsize}*${warehourse}}>> ./tpcc.res
        done
    done
done 

for buffsize in 10 100
do
    for num in 1 2 4 8 10 12 14 16 18
    do
        make clean-data
        ../build/bin/tpcc_bench -n ${num} -d 10 -w ${num} >> ./tpcc.res
    done
done
