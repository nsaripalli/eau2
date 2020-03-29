#pragma once
//lang::CwC

#include <stdlib.h>
#include "array.h"
#include "string.h"
#include "serializableObject.h"

/* This class represents an array.
*  It contains basic methods on array based on the Java arraylist implementation.
*  There are methods to add, remove, and get elements from the list
*
*  author: shah.ash@husky.neu.edu | peters.ci@husky.neu.edu
*  Implementation authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
*/
class StringArray : public SerializableObject {
public:

    Array *internal_array;

    // default constructor
    StringArray() {
        internal_array = new Array();
    }

    StringArray(char *other) {
        char *idx = other;
        while (*idx) {
            String *currStringTok = new String(idx);
            this->append(currStringTok);
            idx += strlen(idx) + 1;
        }
    }

    // destructor
    ~StringArray() {
        delete internal_array;
    }

    StringArray(StringArray *old) {
        this->internal_array = new Array(old->internal_array);
    }

    /* adds an element to the list at the given index
    * @arg o: string to be added to this array
    * @arg index: the index to add the string at
    */
    void add(String *o, size_t index) {
        this->internal_array->add(o, index);
    }

    /* adds an element to the end of the list
    * @arg o: string to be added to this array
    */
    void append(String *o) {
        this->internal_array->append(o);
    }


    /* adds all elements of the given array to this array to the given index
    * @arg a: the array of elements to be added to this list
    * @arg index: the index at which to add the elements of the given array
    */
    void add_all(Array *a, size_t index) {
        this->internal_array->add_all(a, index);
    }


    /* clears the array of all elements
    */
    void clear() {
        this->internal_array->clear();
    }

    /* gets the element in this array at the given index
    * @arg index: the index to get the string at
    */
    String *get(size_t index) {
        Object *output = this->internal_array->get(index);
        if (output == nullptr) return nullptr;
        String *s = dynamic_cast<String *>(output);
        if (s == nullptr) return nullptr;
        return s;
    }

    /* gets the index of the given string, returning the index of the first match.
    * returns the size + 1 of the Array if the object cannot be found
    * @arg o: object to get the index of
    */
    size_t index_of(String *o) {
        return this->internal_array->index_of(o);
    }

    /* removes an element at the given index and returns it
    * @arg index: the index at which to remove the string
    */
    String *remove(size_t index) {
        Object *output = this->internal_array->remove(index);
        if (output == nullptr) return nullptr;
        String *s = dynamic_cast<String *>(output);
        if (s == nullptr) return nullptr;
        return s;
    }

    /* swaps an element at the given index with the given element
    * @arg index: index at which to swap the element
    * @arg o: the string to swap
    */
    String *set(size_t index, String *o) {
        Object *output = this->internal_array->set(index, o);
        if (output == nullptr) return nullptr;
        String *s = dynamic_cast<String *>(output);
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
    * Checks equality between this array and a given string
    * @arg other: the other string to check equality to
    */
    bool equals(Object *other) {
        return this->internal_array->equals(other);
    }

    char *serialize_object_w_size_in_front() {
//        Build up init str with only the payload
        StrBuff interalBuffer;
        for (size_t i = 0; i < this->length(); i++) {
            interalBuffer.c(this->get(i)->serialize_object());
        }
        char* first_output = interalBuffer.val_;
        interalBuffer.val_ = nullptr;

//        Recreate the output with the size in front and then payload
        StrBuff final_output;
        final_output.c(interalBuffer.size_);
        char* only_actual = new char[interalBuffer.size_];
        memcpy(only_actual, first_output, interalBuffer.size_);
        final_output.c(only_actual);

//        Get the final char* out
        char* output = final_output.val_;
        final_output.val_ = nullptr;

        return output;
    }


    char *serialize_object() {
        StrBuff interalBuffer;
        for (size_t i = 0; i < this->length(); i++) {
            interalBuffer.c(this->get(i)->serialize_object());
        }
        interalBuffer.c(nullptr);
        char* output = interalBuffer.val_;
        interalBuffer.val_ = nullptr;
        return output;
    }
};