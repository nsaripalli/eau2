build: 
	g++ -o server -std=c++11 -Wall -g tests/server.cpp -lpthread
	g++ -o client -std=c++11 -Wall -g tests/client.cpp -lpthread
	g++ -o bench -std=c++11 -Wall -g tests/bench.cpp -lpthread
	g++ -o test -std=c++11 -Wall -g tests/testing.cpp -lpthread
	g++ -o app -std=c++11 -Wall -g tests/application_test.cpp -lpthread
	g++ -o personal -std=c++11 -Wall -g tests/personal_test_suite.cpp -lpthread

run-server: 
	@echo "Starting Server..."
	./server -ip 127.0.0.2 
	
run-client:
	@echo "Starting Client 1..."
	./client -ip 127.0.0.3 &
	sleep 4
	@echo "Starting Client 2..."
	./client -ip 127.0.0.4 &
	sleep 4
	@echo "Starting Client 3..."
	./client -ip 127.0.0.5 &
	sleep 4
	@echo "Starting Client 4..."
	./client -ip 127.0.0.6

test:
	./test
	./bench
	./personal
	./app
    (sleep 5; run-client) &
    (cd ../src; run-server) 

valgrind:
	valgrind ./test
	valgrind ./bench
	valgrind ./personal	
	valgrind ./app
