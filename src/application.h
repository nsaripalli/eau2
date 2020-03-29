#pragma once

#include "dataframe.h";
#include "object.h";
#include "KVStore.h";

/**
 * Application :: pre-defines the code for setting up the distributed key value 
 * store storage (maintaining a local key-value store) and query system similar 
 * to how Hadoop has parent implementations for Mappers and Reducers. The 
 * application class is expected to be extended by the customer in order to 
 * interface with the API. The application will also provide context for each 
 * instance of itself for algorithms that require information from the global 
 * state such as dependency to finish. Each application is initialized with a 
 * unique index to identify it's key value store.
 * 
 * Authors: sd & ns
 */
class Application : public Object {
public:
  KVStore kv;

  Application(int idx, char* ip) {
    kv = KVStore(idx, ip, 8080);\
  }

  bool done() {
    return kv.client_->done;
  }

  int this_node() { return kv.idx_; }

  virtual void run_() {}
};

/**
 * Trivial :: A trivial example of an application
 * 
 * Authors: jv, sd, & ns
 */
class Trivial : public Application {
public:
  Trivial(int idx, char* ip) : Application(idx, ip) { assert(idx == 0); } // assumes 0 for M2
  void run_() {
    size_t SZ = 1000*1000;
    int* vals = new int[SZ];
    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key key("triv",0);
    DataFrame* df = DataFrame::fromArray(&key, &kv, SZ, vals);
    assert(df->get_int(0,1) == 1);
    DataFrame* df2 = kv.wait_and_get(key);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_int(0,i);
    assert(sum==0);
    delete[] vals;
    delete df;
  }
};