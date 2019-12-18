all: native-test

native-test:
	g++ -c common.cpp
	g++ -c native-test.cpp
	g++ common.o native-test.o -o native-test -linnodb

ycsb-test: clean
	g++ -c common.cpp
	g++ -c ycsb-test.cpp
	g++ -c ycsb.cpp 
	g++ common.o ycsb-test.o ycsb.o -o ycsb-test -linnodb -pthread
	./ycsb-test

tpcc-test: clean
	g++ -c common.cpp
	g++ -c tpcc-test.cpp
	g++ -c tpcc.cpp 
	g++ -c tpcc-txn.cpp
	g++ -c tpcc-helper.cpp
	g++ common.o tpcc-test.o tpcc.o tpcc-helper.o tpcc-txn.o -o tpcc-test -linnodb -pthread
	./tpcc-test

run:
	./native-test


clean:
	rm -f *.o
	rm -f native-test
	rm -f ycsb-test
	rm -f tpcc-test
	rm -f ibdata*
	rm -rf log
	rm -rf test
	rm -rf TPCC