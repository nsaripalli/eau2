#pragma once

#include "column.h"
#include "stringMetaArray.h"

/*************************************************************************
 * StringColumn::
 * Holds String values.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class StringColumn : public Column {
public:
    StringMetaArray* arr_;

    /** Deletes the metaarray for this column **/
    virtual ~StringColumn() {
        delete arr_;
    }

    StringColumn(char* serialized_object) {
        arr_ = new StringMetaArray(serialized_object);
    }

    /**
     * Default constructor. Creates an empty column with an
     * empty metaarray internally.
     **/
    StringColumn() {
        arr_ = new StringMetaArray();
    }

    /**
     * Constructor with variable number of arguments, specifed by n. 
     * Creates a column with the elements in it in the order given.
     **/
    StringColumn(int n, ...) : StringColumn() {
        va_list args;
        va_start(args, n);
        for (int i=0; i<n; i++) {
            arr_->push_back(va_arg(args, String*));
        }
        va_end(args);
    }

    /** Returns this column **/
    StringColumn* as_string() { return this; }

    /**
     * Adds the given value to the end of this column
     **/
    virtual void push_back(String* val) {
        arr_->push_back(val);
    }
    
    /** The number of elements in this column **/
    size_t size() { return arr_->size(); }

    /** Return the type of this column as a char: 'S' **/
    char get_type_() { return 'S'; }
    
    /** 
     * Returns the element at the given index.
     * An index out of bounds produces undefined behavior.
     **/
    String* get(size_t idx) {
        return arr_->get(idx);
    }

    /** Set value at idx. An out of bound idx is undefined.  */
    void set(size_t idx, String* val) { arr_->set(idx, val); }

    char* serialize_object() {
        return this->arr_->serialize_object();
    }
};
