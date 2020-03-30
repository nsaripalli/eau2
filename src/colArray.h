#pragma once
//lang::CwC

#include <stdlib.h>
#include "array.h"
#include "column.h"

/* This class represents an array.
*  It contains basic methods on array based on the Java arraylist implementation.
*  There are methods to add, remove, and get elements from the list
*
*  author: shah.ash@husky.neu.edu | peters.ci@husky.neu.edu
*  Implementation authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
*/
class ColumnArray : public SerializableObject {
public:

    Array* internal_array;
//    const char* SERIALIZATION_DELIMETER = "\uECCF6e缽ȝ`ſǀƈ嘟ͥ\u1941";
    const char* SERIALIZATION_DELIMETER = "././";



    // default constructor
    ColumnArray() {
        internal_array = new Array();
    }

    ColumnArray(char* serailzied, char* schema) {
        internal_array = new Array();
        // Returns first token
        char* token = strtok(serailzied, SERIALIZATION_DELIMETER);
        int curr_col_idx = 0;

        // Keep printing tokens while one of the
        // delimiters present in str[].
        while (token != NULL) {

            if ('S' == schema[curr_col_idx]) {
                this->internal_array->append(new StringColumn(token));
            }
            if ('B' == schema[curr_col_idx]) {
                this->internal_array->append(new BoolColumn(token));
            }
            if ('I' == schema[curr_col_idx]) {
                this->internal_array->append(new IntColumn(token));
            }
            if ('F' == schema[curr_col_idx]) {
                this->internal_array->append(new FloatColumn(token));
            }


            token = strtok(NULL, SERIALIZATION_DELIMETER);
        }
    }

    // destructor
    ~ColumnArray() {
        delete internal_array;
    }

    ColumnArray(ColumnArray* old) {
        this->internal_array = new Array(old->internal_array);
    }

    /* adds an element to the list at the given index
    * @arg o: column to be added to this array
    * @arg index: the index to add the column at
    */
    void add(Column* o, size_t index) {
        this->internal_array->add(o, index);
    }

    /* adds an element to the end of the list
    * @arg o: column to be added to this array
    */
    void append(Column* o) {
        this->internal_array->append(o);
    }


    /* adds all elements of the given array to this array to the given index
    * @arg a: the array of elements to be added to this list
    * @arg index: the index at which to add the elements of the given array
    */
    void add_all(Array* a, size_t index) {
        this->internal_array->add_all(a, index);
    }


    /* clears the array of all elements
    */
    void clear() {
        this->internal_array->clear();
    }

    /* gets the element in this array at the given index
    * @arg index: the index to get the column at
    */
    Column* get(size_t index) {
        Object* output = this->internal_array->get(index);
        if (output == nullptr) return nullptr;
        Column* s = dynamic_cast<Column*>(output);
        if (s == nullptr) return nullptr;
        return s;
    }

    /* gets the index of the given column, returning the index of the first match.
    * returns the size + 1 of the Array if the object cannot be found
    * @arg o: object to get the index of
    */
    size_t index_of(Column* o) {
        return this->internal_array->index_of(o);
    }

    /* removes an element at the given index and returns it
    * @arg index: the index at which to remove the column
    */
    Column* remove(size_t index) {
        Object* output = this->internal_array->remove(index);
        if (output == nullptr) return nullptr;
        Column *s = dynamic_cast<Column*>(output);
        if (s == nullptr) return nullptr;
        return s;
    }

    /* swaps an element at the given index with the given element
    * @arg index: index at which to swap the element
    * @arg o: the column to swap
    */
    Column* set(size_t index, Column* o) {
        Object* output = this->internal_array->set(index, o);
        if (output == nullptr) return nullptr;
        Column *s = dynamic_cast<Column*>(output);
        if (s == nullptr) return nullptr;
        return s;
    }

    /* returns the number of elements in this array
    */
    size_t length() {
        return this->internal_array->length();
    }

    /*
    * Computes the hash value for this Array
    */
    size_t hash() {
        return this->internal_array->hash();
    }

    /*
    * Checks equality between this array and a given column
    * @arg other: the other column to check equality to
    */
    bool equals(Object* other) {
        return this->internal_array->equals(other);
    }

    Serialized serialize_object() override {
        StrBuff interalBuffer;
        for (size_t i = 0; i < this->length(); i++) {
            Serialized curr_col = this->get(i)->serialize_object();
            interalBuffer.c(curr_col);
            delete [] curr_col.data;
            interalBuffer.c(SERIALIZATION_DELIMETER);
        }

        return interalBuffer.getSerialization();
    }
};