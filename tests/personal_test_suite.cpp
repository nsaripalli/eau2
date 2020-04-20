#include "../src/primatives/object.h"  // Your file with the CwC declaration of Object
#include "../src/columns/column.h"
#include "../src/columns/boolColumn.h"
#include "../src/columns/intColumn.h"
#include "../src/dataframe.h"

#define ASSERT_TRUE(a)  \
  assert((a) == true);
#define ASSERT_FALSE(a) \
  assert((a) == false);
#define ASSERT_EQ(a, b) \
  assert((a) == b);

void test1() {
    Column *simpleCol = new BoolColumn();
    ASSERT_TRUE(simpleCol->as_bool() != nullptr);
    ASSERT_FALSE(simpleCol->as_float() != nullptr);
    ASSERT_FALSE(simpleCol->as_int() != nullptr);
    ASSERT_FALSE(simpleCol->as_string() != nullptr);
    BoolColumn *bc = simpleCol->as_bool();
    for (int i = 0; i < 9999; i++) {
        ASSERT_EQ(bc->size(), i);
        bc->push_back(i % 2 == 0);
    }
    delete simpleCol;
}

void test2() {
    IntColumn *ic = new IntColumn(3, 1, -2, 3);

    ASSERT_EQ(ic->size(), 3);
    ASSERT_EQ(ic->get(1), -2);
    ic->set(1, 324);
    ASSERT_EQ(ic->get(1), 324);
    ic->set(3, -4);
    ASSERT_EQ(ic->get(3), -4);
    ASSERT_EQ(ic->size(), 4);

    delete ic;
}

void test3() {
    Schema s("IBIB");

    DataFrame df(s);
    Row r(df.get_schema());
    for (size_t i = 0; i < 5000; i++) {
        r.set(0, (int) i);
        r.set(1, (bool) true);
        r.set(2, (int) i + 1);
        r.set(3, (bool) false);
        df.add_row(r);
    }
    ASSERT_EQ(df.get_int((size_t) 0, 1), 1);
    ASSERT_EQ(df.get_bool((size_t) 1, 1345), true);
    ASSERT_EQ(df.get_bool((size_t) 3, 4011), false);
}


void test4() {
    Schema s("");

    DataFrame df(s);
    IntColumn ic(1, 2);
    BoolColumn bc(1, true);
    for (int i = 0; i < 150; i++) {
        ic.set(i, i);
        bc.set(i, true);
    }
    String *iname = new String("ints");
    df.add_column(&ic, iname);
    String *bname = new String("bools");
    df.add_column(&bc, bname);
    ASSERT_EQ(df.get_int(0, 12), 12);
    ASSERT_EQ(df.get_bool(1, 103), true);

    Row r(df.get_schema());
    r.set(0, -1);
    r.set(1, false);
    df.add_row(r);
    ASSERT_EQ(df.get_int(0, 150), -1);
    ASSERT_EQ(df.get_bool(1, 150), false);


    delete iname;
    delete bname;
}

void test5() {
    Schema s("F");

    DataFrame df(s);
    Row r(df.get_schema());
    for (size_t i = 0; i < 1000000; i++) {
        r.set(0, (float) (1.3 * i));
        df.add_row(r);
    }

    ASSERT_EQ(df.ncols(), 1);
    ASSERT_EQ(df.nrows(), 1000000);
}

void test6() {
    Schema s("BFISB");

    DataFrame df(s);

    ASSERT_EQ(df.ncols(), 5);
    ASSERT_EQ(df.nrows(), 0);
}

void test7() {
    Schema s("");

    DataFrame df(s);

    ASSERT_EQ(df.ncols(), 0);
    ASSERT_EQ(df.nrows(), 0);
}

int main(int argc, char **argv) {
    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
    test7();
}