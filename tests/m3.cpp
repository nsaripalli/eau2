#include "../src/dataframe.h"
#include "../src/network.h"
#include "../src/application.h"
#include <thread>

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
    size_t SZ = 100*1000;
    float* vals = new float[SZ];
    float sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DataFrame::fromArray(&main, kv, SZ, vals);
    DataFrame::fromScalar(&check, kv, sum);
  }
 
  void counter() {
    DataFrame* v = kv->wait_and_get(main);
    size_t sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_float(0,i);
    p("The sum is  ").pln(sum);
    DataFrame::fromScalar(&verify, kv, sum);
  }
 
  void summarizer() {
    sleep(10);
    DataFrame* result = kv->wait_and_get(verify);
    DataFrame* expected = kv->wait_and_get(check);
    pln(expected->get_float(0,0)==result->get_float(0,0) ? "SUCCESS":"FAILURE");
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