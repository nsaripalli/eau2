#pragma once
//lang::CwC

#include <stdlib.h>
#include "array.h"
#include "../columns/column.h"

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




    // default constructor
    ColumnArray() {
        internal_array = new Array();
    }

    void handleNestedArrayGeneration(char* token, char schema_type) {
        if ('S' == schema_type) {
            this->internal_array->append(new StringColumn(token));
        }
        if ('B' == schema_type) {
            this->internal_array->append(new BoolColumn(token));
        }
        if ('I' == schema_type) {
            this->internal_array->append(new IntColumn(token));
        }
        if ('F' == schema_type) {
            this->internal_array->append(new FloatColumn(token));
        }
    }


    ColumnArray(char* input, char* schema) : ColumnArray() {
        int curr_col_idx = 0;

        size_t size_of_curr_col = 0;
        memcpy(&size_of_curr_col, input, sizeof(size_t));
        char* token=input + sizeof(size_t);

        while (size_of_curr_col != 0) {
            handleNestedArrayGeneration(token, schema[curr_col_idx]);

            size_t size_of_next_col = 0;
            memcpy(&size_of_next_col, token + size_of_curr_col, sizeof(size_t));
            token = token + size_of_curr_col + sizeof(size_t);
            size_of_curr_col = size_of_next_col;
            curr_col_idx++;
        }

    }

    // destructor
    ~ColumnArray() {
//        for (size_t i = 0; i < internal_array->array_size; i++) {
//            if (internal_array->get(i) != nullptr) {
//                delete internal_array->get(i);
//            }
//        }
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
        if (other == nullptr) return false;
        ColumnArray *s = dynamic_cast<ColumnArray*>(other);
        if (s == nullptr) return false;
        return this->internal_array->equals(s->internal_array);
    }

//    The format of this output is essentially a linked list of columns
    Serialized serialize_object() override {
        StrBuff interalBuffer;
        for (size_t i = 0; i < this->length(); i++) {
            Serialized curr_col = this->get(i)->serialize_object();

            char schema_size_serailzied[sizeof(size_t)];
            memset(&schema_size_serailzied, 0, sizeof(size_t));
            memcpy(&schema_size_serailzied, &curr_col.size, sizeof(size_t));

            interalBuffer.c(schema_size_serailzied, sizeof(size_t));
            interalBuffer.c(curr_col);
            delete [] curr_col.data;
        }

        size_t max = 0;
        char schema_size_serailzied[sizeof(size_t)];
        memset(&schema_size_serailzied, 0, sizeof(size_t));
        memcpy(&schema_size_serailzied, &max, sizeof(size_t));

        interalBuffer.c(schema_size_serailzied, sizeof(size_t));

        return interalBuffer.getSerialization();
    }
};