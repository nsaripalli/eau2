#pragma once

#include "column.h"
#include "floatMetaArray.h"

/*************************************************************************
 * FloatColumn::
 * Holds float values.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class FloatColumn : public Column {
public:
    FloatMetaArray* arr_;

    /** Deletes the metaarray for this column **/
    virtual ~FloatColumn() {
        delete arr_;
    }

    /**
     * Default constructor. Creates an empty column with an
     * empty metaarray internally.
     **/
    FloatColumn() {
        arr_ = new FloatMetaArray();
    }

    FloatColumn(char* serialized_object) {
        arr_ = new FloatMetaArray(serialized_object);
    }

    /**
     * Constructor with variable number of arguments, specifed by n. 
     * Creates a column with the elements in it in the order given.
     **/
    FloatColumn(int n, ...) : FloatColumn() {
        va_list args;
        va_start(args, n);
        for (int i=0; i<n; i++) {
            arr_->push_back((float) va_arg(args, double));
        }
        va_end(args);
    }

    /** Returns this column **/
    FloatColumn* as_float() { return this; }

    /**
     * Adds the given value to the end of this column
     **/
    virtual void push_back(float val) {
        arr_->push_back(val);
    }
    
    /** The number of elements in this column **/
    size_t size() { return arr_->size(); }

    /** Return the type of this column as a char: 'F' **/
    char get_type_() { return 'F'; }

    /** 
     * Returns the element at the given index.
     * An index out of bounds produces undefined behavior.
     **/
    float get(size_t idx) {
        return arr_->get(idx);
    }

    /** Set value at idx. An out of bound idx is undefined. */
    void set(size_t idx, float val) { arr_->set(idx, val); }

    Serialized serialize_object() {
        return this->arr_->serialize_object();
    }

    bool equals(Object *other) override {
        if (other == nullptr) return false;
        FloatColumn *s = dynamic_cast<FloatColumn*>(other);
        if (s == nullptr) return false;

        return this->arr_->equals(s->arr_);
    }
};
