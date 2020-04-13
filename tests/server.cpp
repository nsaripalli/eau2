#include "../src/primatives/network.h"
#include <stdio.h> 

int main(int argc, char *argv[]) {

  Server server = Server(argv[2], 8080);
  server.bgStart();
  printf("The server is now running, press enter to initiate a graceful shutdown of all nodes.\n");
  while(getchar() == -1) {
      sleep(1);
  }

  server.shutdown();

  return 0;
}