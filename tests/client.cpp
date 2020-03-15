#include "../src/network.h"
#include "../src/intMetaArray.h"
#include "../src/stringMetaArray.h"

int main(int argc, char *argv[]) {

  Client client = Client(argv[2], 8080);
  // client.start();

  client.bgStart();
  sleep(10);
  String* msg1 = new String("HELLO THERE FRIEND!");
  String* msg2 = new String("GREETINGS!");
  client.sendMessage(msg1);
  sleep(1);
  client.sendMessage(msg2);
  while(!client.done);
  client.shutdown();

  return 0;
}