#include "../src/primatives/network.h"
#include <stdio.h> 

int main(int argc, char *argv[]) {

    pthread_barrier_t   barrier; // the barrier synchronization object
    pthread_barrier_init (&barrier, nullptr, 1);

  Server server = Server(argv[2], 8080);
  server.bgStart(barrier);
  printf("The server is now running, press enter to initiate a graceful shutdown of all nodes.\n");
  while(getchar() == -1) {
      sleep(1);
  }

  server.shutdown();

  return 0;
}