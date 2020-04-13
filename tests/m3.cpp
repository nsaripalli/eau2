#include "../src/dataframe.h"
#include "../src/primatives/network.h"
#include "../src/application.h"
#include <thread>

size_t SIZE_OF_DF = 100*1000;

// To run this make Server on one terminal
// Then cmake m3.
// Once you get SUCESS in the terminal hit enter on the Server Program
// Everything will come to a complete.

class Demo : public Application {
public:
  Key main = Key("main",0);
  Key verify = Key("verif",0);
  Key check = Key("ck",0);
 
  Demo(size_t idx, const char* ip): Application(idx, ip) {}
 
  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    size_t SZ = SIZE_OF_DF;
    int* vals = new int[SZ];
    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DataFrame::fromArray(&main, kv, SZ, vals);
    DataFrame::fromScalar(&check, kv, sum);
  }
 
  void counter() {
    DataFrame* v = kv->wait_and_get(main);
    int sum = 0;
    for (size_t i = 0; i < SIZE_OF_DF; ++i) sum += v->get_int(0,i);
    p("The sum is  ").pln(sum);
    DataFrame::fromScalar(&verify, kv, sum);
  }
 
  void summarizer() {
//      We need to sleep to make sure the other nodes are done computing.
// Yes, this needs to be refactored, so we can request for unfinished data.
// Our main goal this time was just to get the networking done.
//    sleep(10);
    DataFrame* result = kv->wait_and_get(verify);
    DataFrame* expected = kv->wait_and_get(check);
    pln(expected->get_int(0,0)==result->get_int(0,0) ? "SUCCESS":"FAILURE");
  }
};

int main(int argc, char *argv[]) {

  Demo* producer = new Demo(0, "127.0.0.2");
  Demo* counter = new Demo(1, "127.0.0.3");
  Demo* summarizer = new Demo(2, "127.0.0.4");
  producer->run_();
  std::thread t1 = std::thread(&Demo::run_, counter);
  std::thread t2 = std::thread(&Demo::run_, summarizer);
  sleep(5);
  while(!producer->done()); 
  while(!counter->done()); 
  while(!summarizer->done());
  t1.join();
  t2.join();

  return 0;
}