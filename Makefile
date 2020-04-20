build: 
	g++ -o server -std=c++11 -Wall -g tests/server.cpp -lpthread
	g++ -o m3 -std=c++11 -Wall -g tests/m3.cpp -lpthread
	g++ -o wc -std=c++11 -Wall -g tests/WordCount.cpp -lpthread
	g++ -o bench -std=c++11 -Wall -g tests/bench.cpp -lpthread
	g++ -o test -std=c++11 -Wall -g tests/testSerialization.cpp -lpthread
	g++ -o ddf -std=c++11 -Wall -g tests/distributedDataFrameTests.cpp -lpthread 
	g++ -o app -std=c++11 -Wall -g tests/application_test.cpp -lpthread
	g++ -o linus -std=c++11 -Wall -g tests/linus.cpp -lpthread
	g++ -o personal -std=c++11 -Wall -g tests/personal_test_suite.cpp -lpthread

run-server: 
	@echo "Starting Server..."
	./server -ip 127.0.0.1 
	
run-client:
	@echo "Starting Demo..."
	./m3

basic-test:
	./test
	./bench
	./personal
	./app
	./ddf

test:
	./test
	./bench
	./personal
	./app
	./ddf
	sleep 5
	run-client
	run-server

run-wc:
	sleep 5
	run-server
	./wc

run-linus:
	sleep 5
	run-server
	./linus

valgrind:
	valgrind ./test
	valgrind ./bench
	valgrind ./personal	
	valgrind ./app
