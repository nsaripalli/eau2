#include "../src/network.h"
#include "../src/intMetaArray.h"
#include "../src/stringMetaArray.h"
#include "../src/application.h"
#include "../src/KVStore.h"

class Demo : public Application {
public:
  Key main = Key("main",0);
  Key verify = Key("verif",0);
  Key check = Key("ck",0);
 
  Demo(size_t idx, char* ip): Application(idx, ip) {}
 
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
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DataFrame::fromArray(&main, &kv, SZ, vals);
    DataFrame::fromScalar(&check, &kv, sum);
  }
 
  void counter() {
    DataFrame* v = kv.wait_and_get(main);
    size_t sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_float(0,i);
    p("The sum is  ").pln(sum);
    DataFrame::fromScalar(&verify, &kv, sum);
  }
 
  void summarizer() {
    DataFrame* result = kv.wait_and_get(verify);
    DataFrame* expected = kv.wait_and_get(check);
    pln(expected->get_float(0,0)==result->get_float(0,0) ? "SUCCESS":"FAILURE");
  }
};

int main(int argc, char *argv[]) {

  // TODO run demo on 3 different threads with 3 ips that are not 127.0.0.1

  return 0;
}