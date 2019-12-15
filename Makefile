all: native-test

native-test:
	g++ -c common.cpp
	g++ -c native-test.cpp
	g++ common.o native-test.o -o native-test -linnodb

ycsb-test:
	g++ -c common.cpp
	g++ -c ycsb-test.cpp
	g++ -c ycsb.cpp 
	g++ common.o ycsb-test.o ycsb.o -o ycsb-test -linnodb


run:
	./native-test


clean:
	rm -f *.o
	rm -f native-test
	rm -f ycsb-test
	rm -f ibdata*
	rm -rf log
	rm -rf test