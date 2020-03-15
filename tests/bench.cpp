//
// Created by nsaripalli on 2/19/20.
//

#include <cstddef>
#include <algorithm>
#include "../src/intColumn.h"
#include "../src/floatColumn.h"
#include "../src/boolColumn.h"
#include "../src/stringColumn.h"
#include "../src/dataframe.h"
#include "../src/modified_dataframe.h"

int num_columns;
size_t df_len;

class Summer : public Rower {
public:
    size_t sum;

    Summer() {
        sum = 0;
    }

    virtual ~Summer() {
    }

    bool accept(Row &r) {
        for (int colIdx = 0; colIdx + 4 < num_columns; colIdx += 4) {
            sum += r.get_int(0 + colIdx);
        }
        return true;
    }

    void join_delete(Rower *other) {
        sum += dynamic_cast<Summer *>(other)->sum;
        delete other;
    }

    Object *clone() {
        return new Summer();
    }
};

size_t num_complex_iterations;

class ComplexRower : public Rower {
public:
    size_t sum;

    ComplexRower() {
        sum = 0;
    }

    virtual ~ComplexRower() {
    }

    bool accept(Row &r) {
        for (int colIdx = 0; colIdx + 4 < num_columns; colIdx += 4) {
            sum += r.get_int(0 + colIdx);
            sum += (int) r.get_float(1 + colIdx);
            sum += r.get_bool(2 + colIdx);
            for (size_t i = 0; i < num_complex_iterations; i++) {
                sum += r.get_string(3 + colIdx)->hash() % 100;
            }
        }
        return true;
    }

    void join_delete(Rower *other) {
        sum += dynamic_cast<ComplexRower *>(other)->sum;
        delete other;
    }

    Object *clone() {
        return new ComplexRower();
    }
};

int regular_dataframe(Rower &theRower) {

    Schema *schema = new Schema();
    DataFrame *df = new DataFrame(*schema);

    for (int i = 0; i < num_columns; i++) {
        IntColumn *int_col = new IntColumn();
        for (int i = 0; i < df_len; i++) {
            int_col->push_back(i);
        }

        FloatColumn *float_col = new FloatColumn();
        for (float i = 0; i < df_len; i++) {
            float_col->push_back((float) i);
        }

        BoolColumn *bool_col = new BoolColumn();
        for (int i = 0; i < df_len; i++) {
            bool_col->push_back((bool) i % 1);
        }

        StringColumn *string_col = new StringColumn();
        for (int i = 0; i < df_len; i++) {
            String rand_str("testing a normal sentence", 7);
            string_col->push_back(&rand_str);
        }

        df->add_column(int_col, nullptr);
        df->add_column(float_col, nullptr);
        df->add_column(bool_col, nullptr);
        df->add_column(string_col, nullptr);
    }

    struct timespec start, finish;
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &start);

    df->map(theRower);

    clock_gettime(CLOCK_MONOTONIC, &finish);

    elapsed = (finish.tv_sec - start.tv_sec);
    elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

//    printf("elapsed: %f\n", elapsed);
    printf("%f\n", elapsed);

    delete schema;
    delete df;

    return 0;
}


int mregular_dataframe(Rower &theRower) {
    Schema *schema = new Schema();
    MDataFrame *df = new MDataFrame(*schema);

    for (int i = 0; i < num_columns; i++) {
        IntColumn *int_col = new IntColumn();
        for (int i = 0; i < df_len; i++) {
            int_col->push_back(i);
        }

        FloatColumn *float_col = new FloatColumn();
        for (float i = 0; i < df_len; i++) {
            float_col->push_back((float) i);
        }

        BoolColumn *bool_col = new BoolColumn();
        for (int i = 0; i < df_len; i++) {
            bool_col->push_back((bool) i % 1);
        }

        StringColumn *string_col = new StringColumn();
        for (int i = 0; i < df_len; i++) {
            String rand_str("testing a normal sentence", 7);
            string_col->push_back(&rand_str);
        }

        df->add_column(int_col, nullptr);
        df->add_column(float_col, nullptr);
        df->add_column(bool_col, nullptr);
        df->add_column(string_col, nullptr);
    }


    struct timespec start, finish;
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &start);

    df->pmap(theRower);

    clock_gettime(CLOCK_MONOTONIC, &finish);

    elapsed = (finish.tv_sec - start.tv_sec);
    elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

//    printf("elapsed: %f\n", elapsed);
    printf("%f\n", elapsed);

    delete schema;
    delete df;

    return 0;
}

/**
 *
 * @param argc
 * @param argv
 * num_columns how many sets of 4 columns should we have?
 * df_len: how large should the data frame be
 * num_complex_iterations
 * is_simple_or_complex: 0 for simple 1 for complex rower
 * is_regular_dataframe: 1 for multithreaded. 0 for single threaded
 * @return
 */
int main(int argc, char *argv[]) {
    if (argc != 6) {
        puts("Missing arguments in main");
        exit(1);
    }

//    999
    num_columns = atol(argv[1]);
//    10000
    df_len = atol(argv[2]);
//    999
    num_complex_iterations = atol(argv[3]);

    if (atoi(argv[4]) == 0) {

        Summer theRower = Summer();

        if (atoi(argv[5]) == 0) {
            regular_dataframe(theRower);
        } else {
            mregular_dataframe(theRower);
        }

//        size_t final_output = theRower.sum;
//        printf("Final output %zu\n", final_output);

    } else {

        ComplexRower theRower = ComplexRower();

        if (atoi(argv[5]) == 0) {
            regular_dataframe(theRower);
        } else {
            mregular_dataframe(theRower);
        }

//        size_t final_output = theRower.sum;
//        printf("Final output %zu\n", final_output);
    }

    return 0;
}