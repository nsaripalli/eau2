#pragma once

#include "metaArray.h"
#include "../primatives/string.h"
#include "../primatives/strBuff.h"

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

    StringMetaArray(char *input) : StringMetaArray() {
        int curr_col_idx = 0;

        size_t size_of_curr_col = 0;
        memcpy(&size_of_curr_col, input, sizeof(size_t));
        char* token=input + sizeof(size_t);

        while (size_of_curr_col != UINT32_MAX) {
            this->push_back(new String(token));

            size_t size_of_next_col = 0;
            memcpy(&size_of_next_col, token + size_of_curr_col, sizeof(size_t));
            token = token + size_of_curr_col + sizeof(size_t);
            size_of_curr_col = size_of_next_col;
            curr_col_idx++;
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

    Serialized serialize_object() {
        StrBuff internalBuffer;
        for (size_t i = 0; i < this->nextIndex_; i++) {
            Serialized curr_col = this->get(i)->serialize_object();
            char schema_size_serailzied[sizeof(size_t)];
            memset(&schema_size_serailzied, 0, sizeof(size_t));
            memcpy(&schema_size_serailzied, &curr_col.size, sizeof(size_t));

            internalBuffer.c(schema_size_serailzied, sizeof(size_t));
            internalBuffer.c(curr_col);
            delete [] curr_col.data;
        }

        size_t max = UINT32_MAX;
        char schema_size_serailzied[sizeof(size_t)];
        memset(&schema_size_serailzied, 0, sizeof(size_t));
        memcpy(&schema_size_serailzied, &max, sizeof(size_t));

        internalBuffer.c(schema_size_serailzied, sizeof(size_t));

        return internalBuffer.getSerialization();
    }

    bool equals(Object *other) override {
        if (other == nullptr) return false;
        StringMetaArray *s = dynamic_cast<StringMetaArray*>(other);
        if (s == nullptr) return false;
        if ((this->arrsNum_ == s->arrsNum_) && (this->nextIndex_ == s->nextIndex_)) {
            for (size_t i = 0; i < this->nextIndex_; i++) {
                if (!this->get(i)->equals(s->get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
};