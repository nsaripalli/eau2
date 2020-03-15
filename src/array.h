//lang::CwC
#pragma once

#include "object.h"
#include <stdlib.h>
#include <assert.h>
#include <cstring>
#include "errors.h"

/* This class represents an array.
*  It contains basic methods on array based on the Java arraylist implementation.
*  There are methods to add, remove, and get elements from the list
*
*  author: shah.ash@husky.neu.edu | peters.ci@husky.neu.edu
*  Implementation authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
*/
class Array : public Object{
public:

    size_t current_max_index;
    size_t array_size;
    Object **internal_list_;

    Array() {
        array_size = 10;
        internal_list_ = new Object *[array_size];
        memset(internal_list_, 0, array_size * sizeof(Object *));
        this->current_max_index = 0;
    }

    virtual ~Array() {
        delete[] internal_list_;
    }

    Array(Array* old) {
        array_size = old->array_size;
        internal_list_ = new Object *[array_size];
        memcpy(internal_list_, old->internal_list_, array_size * sizeof(Object *));
        this->current_max_index = old->current_max_index;
    }

    // If the array is too small, doubles the array size
    void double_array_size() {
        Object **new_list = new Object *[array_size * 2];
        memset(new_list, 0, array_size * 2 * sizeof(Object *));
        memcpy(new_list, internal_list_, array_size * sizeof(Object *));
        array_size *= 2;
        delete[] internal_list_;
        internal_list_ = new_list;
    }

    /* adds an element to the list at the given index
    * shifts the element currently at that position to the right
    * @arg o: object to be added to this array
    * @arg index: the index to add the object at
    */
    void add(Object* o, size_t index) {
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
               (this->current_max_index - index) * sizeof(Object *));
        memset(&this->internal_list_[index], 0, 1 * sizeof(Object *));
        this->current_max_index++;

        internal_list_[index] = o;
    }

    /* adds an element to the end of the list
    * @arg o: object to be added to this array
    */
    void append(Object* o) {
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
               (this->current_max_index - index) * sizeof(Object *));
        memset(&this->internal_list_[index], 0, a->length() * sizeof(Object *));
        memcpy(&this->internal_list_[index], &a->internal_list_[0], a->length() * sizeof(Object *));

        this->current_max_index += a->length();
    }


    /* clears the array of all elements
    */
    void clear() {
        for (size_t i = 0; i < this->current_max_index; i++) {
            if (internal_list_[i] != nullptr) {
                internal_list_[i] = nullptr;
            }
        }
        this->current_max_index = 0;
    }

    /* gets the element in this array at the given index
    * @arg index: the index to get the object at
    */
    Object* get(size_t index) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        if (index >= this->current_max_index) {
            return nullptr;
        }

        return internal_list_[index];
    }

    /* gets the index of the given object, returning the index of the first match.
    * returns the size + 1 of the Array if the object cannot be found
    * @arg o: object to get the index of
    */
    size_t index_of(Object* o) {
        for (size_t i = 0; i < this->current_max_index; i++) {
            if (internal_list_[i]->equals(o)) {
                return i;
            }
        }

        return this->length() + 1;
    }

    /* removes an element at the given index and returns it
    * @arg index: the index at which to remove the object
    */
    Object* remove(size_t index) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        if (index >= this->current_max_index) {
            return nullptr;
        }

        Object *output = internal_list_[index];

        this->current_max_index--;

        //        Shift over the array by one down at index i
        memmove(&this->internal_list_[index], &this->internal_list_[index + 1],
                (this->current_max_index - index) * sizeof(Object *));

        return output;
    }

    /* swaps an element at the given index with the given element
    * @arg index: index at which to swap the element
    * @arg o: the object to swap
    */
    Object* set(size_t index, Object* o) {
        if (index < 0) {
            indexOutOfBounds(index, this->length());
        }

        while (index >= array_size) {
            double_array_size();
        }

        Object *output = this->remove(index);
        this->add(o, index);
        return output;
    }

    /**
     * Checks object equality to the given object
     * @param other the object to compare to
     */
    virtual bool equals(Object *other) {
        if (other == nullptr) return false;
        Array *s = dynamic_cast<Array*>(other);
        if (s == nullptr) return false;

        if (s->length() != this->length()) {
            return false;
        }

        int other_index = 0;
        for (size_t i = 0; i < this->current_max_index; i++) {
            if (internal_list_[i] != nullptr && s->get(i) != nullptr) {
                if (not internal_list_[i]->equals(s->get(i))) {
                    return false;
                }
            }
//            This means both are not nullptrs even though one of them is.
            else if (internal_list_[i] != s->get(i)) {
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
        size_t total_hash = 19;
        int prime = 5;
        for (size_t i = 0; i < this->current_max_index; i++) {
            if (internal_list_[i] != nullptr) {
                total_hash = (total_hash*prime) + internal_list_[i]->hash();
            }
        }
        return total_hash;

    }  // Returns the hash code value for this list.

};
