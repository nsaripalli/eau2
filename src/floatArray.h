//lang::CwC
#pragma once

#include "object.h"
#include <stdlib.h>
#include <assert.h>
#include <cstring>
#include "errors.h"

/* This class represents an float array.
*  It contains basic methods on array based on the Java arraylist implementation.
*  There are methods to add, remove, and get elements from the list
*
*  author: shah.ash@husky.neu.edu | peters.ci@husky.neu.edu
*  Implementation authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
*/
class FloatArray : public Object{
public:

    size_t current_max_index;
    size_t array_size;
    float *internal_list_;

    FloatArray() {
        array_size = 10;
        internal_list_ = new float[array_size];
        memset(internal_list_, 0, array_size * sizeof(float));
        this->current_max_index = 0;
    }

    FloatArray(char* buff) {
        current_max_index = 0;
        array_size = 0;

        char* fullInput = buff;

        const char s[2] = "\0";
        char *token;

        memcpy(&current_max_index, fullInput, sizeof(current_max_index));

        memcpy(&array_size,  fullInput + sizeof(current_max_index), sizeof(array_size));

        internal_list_ = new float[array_size];

        char* outsideInternals = fullInput + sizeof(current_max_index) + sizeof(array_size);
        memcpy(internal_list_, outsideInternals, array_size * sizeof(float));
    }

    virtual ~FloatArray() {
        delete[] internal_list_;
    }

    FloatArray(FloatArray* old) {
        array_size = old->array_size;
        internal_list_ = new float[array_size];
        memcpy(internal_list_, old->internal_list_, array_size * sizeof(float));
        this->current_max_index = old->current_max_index;
    }

    // If the array is too small, doubles the array size
    void double_array_size() {
        float*new_list = new float[array_size * 2];
        memset(new_list, 0, array_size * 2 * sizeof(float));
        memcpy(new_list, internal_list_, array_size * sizeof(float));
        array_size *= 2;
        delete[] internal_list_;
        internal_list_ = new_list;
    }

    /* adds an element to the list at the given index
    * @arg o: object to be added to this array
    * @arg index: the index to add the object at 
    */
    void add(float o, size_t index) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        while (index >= array_size) {
            double_array_size();
        }

        if (this->current_max_index <= index) {
            this->current_max_index = index + 1;
            internal_list_[index] = o;
            return;
        }


//        Shift over the array by one at index i
        memcpy(&this->internal_list_[index + 1], &this->internal_list_[index],
               (this->current_max_index - index) * sizeof(float));
        memset(&this->internal_list_[index], 0, 1 * sizeof(float));
        this->current_max_index++;

        internal_list_[index] = o;
    }

    /* adds an element to the end of the list
    * @arg o: object to be added to this array
    */
    void append(float o) {
        if (this->current_max_index >= array_size) {
            double_array_size();
        }

        this->current_max_index++;
        internal_list_[this->current_max_index - 1] = o;
    }


    /* adds all elements of the given array to this array to the given index
    * @arg a: the array of elements to be added to this list
    * @arg index: the index at which to add the elements of the given array
    */
    void add_all(Array* a, size_t index) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        while (a->length() + this->current_max_index >= array_size) {
            this->double_array_size();
        }
//        Shift over the current list at i by c size
        memcpy(&this->internal_list_[index + a->length()], &this->internal_list_[index],
               (this->current_max_index - index) * sizeof(float));
        memset(&this->internal_list_[index], 0, a->length() * sizeof(float));
        memcpy(&this->internal_list_[index], &a->internal_list_[0], a->length() * sizeof(float));

        this->current_max_index += a->length();
    }


    /* clears the array of all elements
    */
    void clear() {
        for (size_t i = 0; i < this->current_max_index; i++) {
            internal_list_[i] = false;
        }
        this->current_max_index = 0;
    }

    /* gets the element in this array at the given index
    * @arg index: the index to get the object at
    */
    float get(size_t index) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        if (index >= this->current_max_index) {
            indexOutOfBounds(index, this->length());
        }

        return internal_list_[index];
    }

    /* gets the index of the given float, returning the index of the first match.
    * returns the size + 1 of the Array if the object cannot be found
    * @arg o: object to get the index of
    */
    size_t index_of(float o) {
        for (size_t i = 0; i < this->current_max_index; i++) {
            if (internal_list_[i] == (o)) {
                return i;
            }
        }

        return this->length() + 1;
    }

    /* removes an element at the given index and returns it
    * @arg index: the index at which to remove the object
    */
    float remove(size_t index) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        if (index >= this->current_max_index) {
            indexOutOfBounds(index, this->length());
        }

        float output = internal_list_[index];

        this->current_max_index--;

        //        Shift over the array by one down at index i
        memmove(&this->internal_list_[index], &this->internal_list_[index + 1],
                (this->current_max_index - index) * sizeof(float));

        return output;
    }

    /* swaps an element at the given index with the given element
    * @arg index: index at which to swap the element
    * @arg o: the object to swap
    */
    float set(size_t index, float o) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        while (index >= array_size) {
            double_array_size();
        }

        float output = this->remove(index);
        this->add(o, index);
        return output;
    }

    /**
     * Checks object equality to the given object
     * @param other the object to compare to
     */
    virtual bool equals(Object* other) {
        if (other == nullptr) return false;
        FloatArray *s = dynamic_cast<FloatArray*>(other);
        if (s == nullptr) return false;

        if (s->length() != this->length()) {
            return false;
        }

        int other_index = 0;
        for (size_t i = 0; i < this->current_max_index; i++) {
            if (internal_list_[i] != s->get(i)) {
                return false;
            }
            other_index++;
        }

        return true;
    }

    /* returns the number of elements in this array
    */
    size_t length() {
        return this->current_max_index;
    }

    size_t hash() {
        return this->current_max_index;
    }  // Returns the hash code value for this list.

    char * serialize_object() {
        char* totalBytesArray = new char[sizeof(current_max_index) + sizeof(array_size) + (array_size * sizeof(float))];
        char* bytesCurrent_max_index = totalBytesArray;
        char* bytesArray_size = totalBytesArray + sizeof(current_max_index);
        char*internalListSerialization = totalBytesArray + sizeof(current_max_index) + sizeof(array_size);

        memcpy(bytesCurrent_max_index, &current_max_index, sizeof(current_max_index));

        memcpy(bytesArray_size, &array_size, sizeof(array_size));

        memcpy(internalListSerialization, internal_list_, array_size * sizeof(float));

        return totalBytesArray;
    }

};
