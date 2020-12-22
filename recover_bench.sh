make clean-data
make ycsb-test
./ycsb-test -R -n4 > /dev/null
./ycsb-test -R | grep "total reboot time" > ./ycsb-crash-bench.res
make clean