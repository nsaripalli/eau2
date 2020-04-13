#include <gtest/gtest.h>

#include "../src/primatives/object.h"  // Your file with the CwC declaration of Object
#include "../src/columns/column.h"
#include "../src/columns/boolColumn.h"
#include "../src/columns/intColumn.h"
#include "../src/dataframe.h"

#define ASSERT_TRUE(a)  \
  ASSERT_EQ((a),true);
#define ASSERT_FALSE(a) \
  ASSERT_EQ((a),false);
#define ASSERT_EXIT_ZERO(a)  \
  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

void test1() {
  Column* simpleCol = new BoolColumn();
  ASSERT_TRUE(simpleCol->as_bool() != nullptr);
  ASSERT_FALSE(simpleCol->as_float() != nullptr);
  ASSERT_FALSE(simpleCol->as_int() != nullptr);
  ASSERT_FALSE(simpleCol->as_string() != nullptr);
  BoolColumn *bc = simpleCol->as_bool();
  for (int i=0; i<9999; i++) {
    ASSERT_EQ(bc->size(), i);
    bc->push_back(i%2==0);  
  }
  delete simpleCol;
  exit(0);
}

TEST(W1, test1) {
  ASSERT_EXIT_ZERO(test1);
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
  exit(0);
}

TEST(W1, test2) {
  ASSERT_EXIT_ZERO(test2);
}

void test3() {
  Schema s("IBIB");

  DataFrame df(s);
  Row  r(df.get_schema());
  for(size_t i = 0; i <  5000; i++) {
    r.set(0,(int)i);
    r.set(1,(bool)true);
    r.set(2,(int)i+1);
    r.set(3,(bool)false);
    df.add_row(r);
  }
  ASSERT_EQ(df.get_int((size_t)0,1), 1);
  ASSERT_EQ(df.get_bool((size_t)1,1345), true);
  ASSERT_EQ(df.get_bool((size_t)3,4011), false);
  exit(0);
}

TEST(W1, test3) {
  ASSERT_EXIT_ZERO(test3);
}

void test4() {
  Schema s("");

  DataFrame df(s);
  IntColumn ic(1, 2);
  BoolColumn bc(1, true);
  for (int i=0; i<150; i++) {
      ic.set(i, i);
      bc.set(i, true);
  }
  String* iname = new String("ints");
  df.add_column(&ic, iname);
  String* bname = new String("bools");
  df.add_column(&bc, bname);
  ASSERT_EQ(df.get_int(0, 12), 12);
  ASSERT_EQ(df.get_bool(1, 103), true);

  Row  r(df.get_schema());
  r.set(0, -1);
  r.set(1, false);
    df.add_row(r);
    ASSERT_EQ(df.get_int(0, 150), -1);
    ASSERT_EQ(df.get_bool(1, 150), false);


  delete iname;
  delete bname;
  exit(0);
}

TEST(W1, test4) {
  ASSERT_EXIT_ZERO(test4);
}

void test5() {
  Schema s("F");

  DataFrame df(s);
  Row  r(df.get_schema());
  for(size_t i = 0; i < 1000000; i++) {
    r.set(0,(float)(1.3*i));
    df.add_row(r);
  }

  ASSERT_EQ(df.ncols(), 1000000);
  ASSERT_EQ(df.nrows(), 1);
}

TEST(W1, test5) {
  ASSERT_EXIT_ZERO(test5);
}

void test6() {
  Schema s("BFISB");

  DataFrame df(s);

  ASSERT_EQ(df.ncols(), 5);
  ASSERT_EQ(df.nrows(), 0);
}

TEST(W1, test6) {
  ASSERT_EXIT_ZERO(test6);
}

void test7() {
  Schema s("");

  DataFrame df(s);

  ASSERT_EQ(df.ncols(), 0);
  ASSERT_EQ(df.nrows(), 0);
}

TEST(W1, test7) {
  ASSERT_EXIT_ZERO(test7);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}