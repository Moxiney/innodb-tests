#!/bin/bash

#data size 5GB
#for buffsize in 700 6000
#do
#    for bench in 10 50 90
#    do
#        for num in 1 2 4 6 8 10 12 14 16 18
#        do
#            make clean-data
#            ./ycsb-test -n ${num} -d 10 -k 50000000 -r ${bench} -b ${buffsize} >> ./ycsb.res
#        done
#    done
#done

#data size per wh may be 80MB
# for buffsize in 15 150
# do
#    for warehourse in 1 4
#    do
#        for num in 1 2 4 6 8 10 12 14 16 18
#        do
#            make clean-data
#            ./tpcc-test -n ${num} -d 10 -w ${warehourse} -b $((${warehourse}*${buffsize}))>> ./tpcc3.res
#        done
#    done
# done 

# for buffsize in 15 150
# do
#     for num in 1 2 4 6 8 10 12 14 16 18
#     do
#         make clean-data
#         ./tpcc-test -n ${num} -d 10 -w ${num} -b $((${num}*${buffsize}))>> ./tpcc3.res
#     done
# done



# for buffsize in 2000 20000
# do
#    for bench in 10 50 90
#    do
#        for num in 1 2 4 6 8 10 12 14 16 18
#        do
#            make clean-data
#            ./ycsb-test -n ${num} -d 10 -k 50000000 -r ${bench} -b ${buffsize} >> ./ycsb5.res
#        done
#    done
# done



for warehourse in 1 4
do
    for num in 1 2 4 6 8 10 12 14 16 18
    do
        make clean-data
        ./tpcc-test -n ${num} -d 10 -w ${warehourse} -b 20000>> ./tpcc4.res
    done
done



for num in 1 2 4 6 8 10 12 14 16 18
do
    make clean-data
    ./tpcc-test -n ${num} -d 10 -w ${num} -b 20000>> ./tpcc4.res
done

