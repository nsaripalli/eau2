#pragma once

#include "metaArray.h"
#include "string.h"

/*
 * StringMetaArray ::
 * String typed meta array.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class StringMetaArray : public MetaArray {
public:
    String*** arrs_;

    /* delete arrs_ */
    virtual ~StringMetaArray() {
        for (int i=0; i<arrsNum_; i++)
            delete[] arrs_[i];
        delete[] arrs_;
    }

    /* Creates a new empty StringMetaArray with 1 inner array */
    StringMetaArray() {
        arrs_ = new String**[1];
        arrs_[0] = new String*[arrSize_]; 
        arrsNum_ = 1;
        nextIndex_ = 0;
    }

    StringMetaArray(char *other) : StringMetaArray() {
        char *idx = other;
        while (*idx) {
            String *currStringTok = new String(idx);
            this->push_back(currStringTok);
            idx += strlen(idx) + 1;
        }
    }

    /*
     * Adds the element to the end of the meta array (the next available index).
     * Resizes the meta array by adding another array if the meta array is at 
     * capacity (all the inner arrays are full).
     */
    void push_back(String* sElem) {
        size_t arrIdx = nextIndex_ / arrSize_; // which array is the next index in
        size_t innerIdx = nextIndex_ % arrSize_; // which index is the next index at in that^ array
        
        // do we need to add a new array / are we out of room? 
        if (nextIndex_ == arrsNum_ * arrSize_) {
            String*** newArrs = new String**[arrsNum_ + 1];
            for (int i=0; i<arrsNum_; i++) {
                newArrs[i] = arrs_[i];
            }
            newArrs[arrsNum_] = new String*[arrSize_];
            arrsNum_ += 1;
            String*** temp = arrs_;
            arrs_ = newArrs;
            delete[] temp;
        }

        arrs_[arrIdx][innerIdx] = sElem;
        nextIndex_ += 1;
    }

    /* Returns the String at the given index as if this was one normal, linear array */
    String* get(size_t idx) {
        size_t arrIdx = idx / arrSize_; // which array is the index in
        size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
        return arrs_[arrIdx][innerIdx];
    }

    /* 
     * Updates the String at the given index to be the given val as if this was one 
     * normal, linear array
     */ 
    void set(size_t idx, String* val) {
        if (idx == nextIndex_) 
            push_back(val);
        else {
            size_t arrIdx = idx / arrSize_; // which array is the index in
            size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
            arrs_[arrIdx][innerIdx] = val;
        }
    }

    char *serialize_object() {
        StrBuff interalBuffer;
        for (size_t i = 0; i < this->nextIndex_; i++) {
            interalBuffer.c(this->get(i)->serialize_object());
        }
        char* output = interalBuffer.val_;
        interalBuffer.val_ = nullptr;
        return output;
    }
};