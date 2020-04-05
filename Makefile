build: 
	g++ -o server -std=c++11 -Wall -g tests/server.cpp -lpthread
	g++ -o m3 -std=c++11 -Wall -g tests/m3.cpp -lpthread
	g++ -o bench -std=c++11 -Wall -g tests/bench.cpp -lpthread
	g++ -o test -std=c++11 -Wall -g tests/testSerialization.cpp -lpthread
	g++ -o app -std=c++11 -Wall -g tests/application_test.cpp -lpthread
	g++ -o personal -std=c++11 -Wall -g tests/personal_test_suite.cpp -lpthread

run-server: 
	@echo "Starting Server..."
	./server -ip 127.0.0.1 
	
run-client:
	@echo "Starting Demo..."
	./m3

test:
	./test
	./bench
	./personal
	./app
	sleep 5
	./run-client
	cd ../src
	run-server

valgrind:
	valgrind ./test
	valgrind ./bench
	valgrind ./personal	
	valgrind ./app
