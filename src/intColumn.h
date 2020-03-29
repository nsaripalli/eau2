#pragma once

#include "column.h"
#include "intMetaArray.h"

/*************************************************************************
 * IntColumn::
 * Holds int values.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class IntColumn : public Column {
public:
    IntMetaArray* arr_;

    /** Deletes the metaarray for this column **/
    virtual ~IntColumn() {
        delete arr_;
    }

    /**
     * Default constructor. Creates an empty column with an
     * empty metaarray internally.
     **/
    IntColumn() {
        arr_ = new IntMetaArray();
    }

    IntColumn(char* serialized_object) {
        arr_ = new IntMetaArray(serialized_object);
    }

    /**
     * Constructor with variable number of arguments, specifed by n. 
     * Creates a column with the elements in it in the order given.
     **/
    IntColumn(int n, ...) : IntColumn() {
        va_list args;
        va_start(args, n);
        for (int i=0; i<n; i++) {
            arr_->push_back(va_arg(args, int));
        }
        va_end(args);
    }

    /** Returns this column **/
    IntColumn* as_int() { return this; }

    /**
     * Adds the given value to the end of this column
     **/
    virtual void push_back(int val) {
        arr_->push_back(val);
    }
    
    /** The number of elements in this column **/
    size_t size() { return arr_->size(); }

    /** Return the type of this column as a char: 'I' **/
    char get_type_() { return 'I'; }

    /** 
     * Returns the element at the given index.
     * An index out of bounds produces undefined behavior.
     **/
    int get(size_t idx) {
        return arr_->get(idx);
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, int val) { arr_->set(idx, val); }

    char* serialize_object() {
        return this->arr_->serialize_object();
    }
};
