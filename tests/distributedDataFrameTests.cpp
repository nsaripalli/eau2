#import "testingDistributedDataFrame.cpp"


//int testAddRow() {
//    Schema s("IBIB");
//
////    DummyKVStore stack_d_kv;
//    KVType* dkv = new DummyKVStore();
//    DistributedDataFrame df(s, 3, *dkv, new String("Testing"));
//    Row  r(df.get_schema());
//    for(size_t i = 0; i <  5000; i++) {
//        r.set(0,(int)i);
//        r.set(1,(bool)true);
//        r.set(2,(int)i+1);
//        r.set(3,(bool)false);
//        df.add_row(r);
//    }
//    assert(df.get_int((size_t)0,1) == 1);
//    assert(df.get_bool((size_t) 1, 1345));
//    assert(!df.get_bool((size_t) 3, 4011));
//}

int testSimple() {
    Schema s("IBIB");

    DummyKVStore* dkv = new DummyKVStore();
    DistributedDataFrame df(s, 3, *dkv, new String("Testing"));
    for(size_t i = 0; i <  5000; i++) {
        df.set(0,i,(int)i);
        df.set(1,i,(bool)true);
        df.set(2,i,(int)i+1);
        df.set(3,i,(bool)false);
    }


    DataFrame normalDF(s);
    for(size_t i = 0; i <  5000; i++) {
        normalDF.set(0, i, (int)i);
        normalDF.set(1, i, (bool)true);
        normalDF.set(2, i, (int)i + 1);
        normalDF.set(3, i, (bool)false);
    }

    assert(df.get_int(0, 1) == 1);
    assert(df.get_bool(1, 1345));
    assert(!df.get_bool(3, 4011));

    assert(df.nrows() == normalDF.nrows());
    assert(df.ncols() == normalDF.ncols());
}

int main() {
    testSimple();
}