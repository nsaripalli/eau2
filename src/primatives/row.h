#pragma once

#include "schema.h"
#include "../array/array.h"
#include "../array/intArray.h"
#include "../array/boolArray.h"
#include "../array/floatArray.h"
#include "../array/stringArray.h"
#include "integer.h"
#include "fielder.h"

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
public:
//    Note this is the address to the schema
    Schema* schema;
//    Due to the lack of generics, this is a 2-D array where each nested array is only of one object.
    Array* row;
    size_t index;

    /** Build a row following a schema. */
    Row(Schema &scm) {
        this->row = new Array();
        this->schema = new Schema(scm);
    }

    virtual ~Row() {
        for (int i = 0; i < this->schema->width(); i++) {
            delete this->row->get(i);
        }
        delete this->row;
        delete this->schema;
    }

    /** Setters: set the given column with the given value. Setting a column with
      * a value of the wrong type is undefined. */
    void set(size_t col, int val) {
        Integer* wrapper = new Integer(val);
        Object* old = this->row->set(col, wrapper);
        delete old;
    }

    void set(size_t col, float val) {
        FloatArray* wrapper = new FloatArray();
        wrapper->append(val);
        Object* old = this->row->set(col, wrapper);
        delete old;
    }

    void set(size_t col, bool val) {
        BoolArray* wrapper = new BoolArray();
        wrapper->append(val);
        Object* old = this->row->set(col, wrapper);
        delete old;
    }

    /** String is external */
    void set(size_t col, String *val) {
        StringArray* wrapper = new StringArray();
        wrapper->append(val);
        Object* old = this->row->set(col, wrapper);
        delete old;
    }

    /** Set/get the index of this row (ie. its position in the dataframe. This is
     *  only used for informational purposes, unused otherwise */
    void set_idx(size_t idx) {
        this->index = idx;
    }

    size_t get_idx() {
        return this->index;
    }

    /** Getters: get the value at the given column. If the column is not
      * of the requested type, the result is undefined. */
    int get_int(size_t col) {
        Object* colAtIndex = this->row->get(col);
        Integer* intColAtIndex = dynamic_cast<Integer *>(colAtIndex);
        return intColAtIndex->getInner();
    }

    bool get_bool(size_t col) {
        Object* colAtIndex = this->row->get(col);
        BoolArray* typedColAtIndex = dynamic_cast<BoolArray *>(colAtIndex);
        return typedColAtIndex->get(0);
    }

    float get_float(size_t col) {
        Object* colAtIndex = this->row->get(col);
        FloatArray* typedColAtIndex = dynamic_cast<FloatArray *>(colAtIndex);
        return typedColAtIndex->get(0);
    }

    String *get_string(size_t col) {
        Object* colAtIndex = this->row->get(col);
        StringArray* typedColAtIndex = dynamic_cast<StringArray *>(colAtIndex);
        return typedColAtIndex->get(0);
    }

    /** Number of fields in the row. */
    size_t width() {
        return this->schema->width();
    }

    /** Type of the field at the given position. An idx >= width is  undefined. */
    char col_type(size_t idx) {
        return this->schema->col_type(idx);
    }

    /** Given a Fielder, visit every field of this row. The first argument is
      * index of the row in the dataframe.
      * Calling this method before the row's fields have been set is undefined. */
    void visit(size_t idx, Fielder &f) {
        f.start(idx);

//        Iterate through each of the elements in the row and give them to Fielder
        for (int i = 0; i < this->schema->width(); i++) {
            char curr_col_type = schema->col_type(i);
            if ('S' == curr_col_type) {
                f.accept(this->get_string(i));
            }
            if ('B' == curr_col_type) {
                f.accept(this->get_bool(i));
            }
            if ('I' == curr_col_type) {
                f.accept(this->get_int(i));
            }
            if ('F' == curr_col_type) {
                f.accept(this->get_float(i));
            }
        }

        f.done();
    }
};