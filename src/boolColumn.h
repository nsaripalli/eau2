#pragma once

#include "column.h"
#include "boolMetaArray.h"

/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class BoolColumn : public Column {
public:
    BoolMetaArray* arr_;

    /** Deletes the metaarray for this column **/
    virtual ~BoolColumn() {
        delete arr_;
    }

    /**
     * Default constructor. Creates an empty column with an
     * empty metaarray internally.
     **/
    BoolColumn() {
        arr_ = new BoolMetaArray();
    }

    BoolColumn(char* serialized_object) {
        arr_ = new BoolMetaArray(serialized_object);
    }

    /**
     * Constructor with variable number of arguments, specifed by n. 
     * Creates a column with the elements in it in the order given.
     **/
    BoolColumn(int n, ...) : BoolColumn() {
        va_list args;
        va_start(args, n);
        for (int i=0; i<n; i++) {
            arr_->push_back((bool)va_arg(args, int));
        }
        va_end(args);
    }

    /** Returns this column **/
    BoolColumn* as_bool() { return this; }

    /**
     * Adds the given value to the end of this column
     **/
    virtual void push_back(bool val) {
        arr_->push_back(val);
    }
    
    /** The number of elements in this column **/
    size_t size() { return arr_->size(); }

    /** Return the type of this column as a char: 'B' **/
    char get_type_() { return 'B'; }

    /** 
     * Returns the element at the given index.
     * An index out of bounds produces undefined behavior.
     **/
    bool get(size_t idx) {
        return arr_->get(idx);
    }

    /** Set value at idx. An out of bound idx is undefined. */
    void set(size_t idx, bool val) { arr_->set(idx, val); }

    Serialized serialize_object() {
        return this->arr_->serialize_object();
    }
};
