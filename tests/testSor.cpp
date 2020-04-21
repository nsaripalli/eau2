#include "../src/sorer/sor.h"


void testBoolSor() {
    SorAdapter sa = SorAdapter(0, 500, "bool.sor");
    DataFrame* bdf = sa.get_df();
    assert(bdf->nrows() == 5);
    assert(bdf->ncols() == 3);

    assert(bdf->get_bool(0, 0) == 1);
    assert(bdf->get_bool(2, 4) == 0);
    assert(bdf->get_bool(1, 2) == 0);

    delete bdf;
}

void testIntSor() {
    SorAdapter sa = SorAdapter(0, 500, "int.sor");
    DataFrame* idf = sa.get_df();
    assert(idf->nrows() == 5);
    assert(idf->ncols() == 3);
    assert(idf->get_int(0, 0) == 1);
    assert(idf->get_int(0, 2) == -1);
    assert(idf->get_int(1, 3) == 2);
    assert(idf->get_int(2, 4) == 3);

    delete idf;
}

void testMixedSor() {
    SorAdapter sa = SorAdapter(0, 500, "mixed.sor");
    DataFrame* mdf = sa.get_df();
    assert(mdf->nrows() == 5);
    assert(mdf->ncols() == 3);
    assert(mdf->get_int(0, 0) == 1);
    assert(mdf->get_int(0, 2) == -1);
    assert(mdf->get_float(1, 3) == 2.5);
    assert(mdf->get_float(1, 2) == 0);
    String* zero = new String("0");
    String* hello = new String("hello");
    assert(mdf->get_string(2, 2)->equals(zero));
    assert(mdf->get_string(2, 3)->equals(hello));

    for (size_t i = 0; i< mdf->nrows(); i++) {
        delete mdf->columns->get(2)->as_string()->get(i);
    }
    delete mdf;
    delete zero;
    delete hello;
}

int main() {
    testBoolSor();
    printf("BOOL SUCCESS\n");
    testIntSor();
    printf("INT SUCCESS\n");
    testMixedSor();
    printf("MIXED SUCCESS\n");

    return 0;
}