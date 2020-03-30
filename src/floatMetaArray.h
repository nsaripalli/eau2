#pragma once

#include "metaArray.h"

/*
 * FloatMetaArray ::
 * float typed meta array.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class FloatMetaArray : public MetaArray {
public:
    float** arrs_;

    /* delete arrs_ */
    virtual ~FloatMetaArray() {
        for (int i=0; i<arrsNum_; i++)
            delete[] arrs_[i];
        delete[] arrs_;
    }

    /* Creates a new empty floatMetaArray with 1 inner array */
    FloatMetaArray() {
        arrs_ = new float*[1];
        arrs_[0] = new float[arrSize_]; 
        arrsNum_ = 1;
        nextIndex_ = 0;
    }

    FloatMetaArray (char* totalBytesArray) : FloatMetaArray() {
        memcpy(&arrsNum_, totalBytesArray, sizeof(arrsNum_));
        memcpy(&nextIndex_, totalBytesArray + sizeof(arrsNum_), sizeof(nextIndex_));

        delete[] arrs_[0];
        delete[] arrs_;
        arrs_ = new float*[arrsNum_];

        for (int i =0 ; i < arrsNum_; i++) {
            float* curr_arrs = new float[arrSize_];
            arrs_[i] = curr_arrs;
            memcpy(arrs_[i],
                   totalBytesArray + sizeof(arrsNum_) + sizeof(nextIndex_) + (i * sizeof(float) * arrSize_),
                   sizeof(float) * arrSize_);
        }
    }

    /*
     * Adds the element to the end of the meta array (the next available index).
     * Resizes the meta array by adding another array if the meta array is at 
     * capacity (all the inner arrays are full).
     */
    void push_back(float fElem) {
        size_t arrIdx = nextIndex_ / arrSize_; // which array is the next index in
        size_t innerIdx = nextIndex_ % arrSize_; // which index is the next index at in that^ array
        
        // do we need to add a new array / are we out of room? 
        if (nextIndex_ == arrsNum_ * arrSize_) {
            float** newArrs = new float*[arrsNum_ + 1];
            for (int i=0; i<arrsNum_; i++) {
                newArrs[i] = arrs_[i];
            }
            newArrs[arrsNum_] = new float[arrSize_];
            arrsNum_ += 1;
            float** temp = arrs_;
            arrs_ = newArrs;
            delete[] temp;
        }

        arrs_[arrIdx][innerIdx] = fElem;
        nextIndex_ += 1;
    }

    /* Returns the float at the given index as if this was one normal, linear array */
    float get(size_t idx) {
        size_t arrIdx = idx / arrSize_; // which array is the index in
        size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
        return arrs_[arrIdx][innerIdx];
    }

    /* 
     * Updates the float at the given index to be the given val as if this was one 
     * normal, linear array
     */ 
    void set(size_t idx, float val) {
        if (idx == nextIndex_) 
            push_back(val);
        else {
            size_t arrIdx = idx / arrSize_; // which array is the index in
            size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
            arrs_[arrIdx][innerIdx] = val;
        }
    }

    char *serialize_object() {
        char* totalBytesArray = new char[sizeof(arrsNum_) + sizeof(nextIndex_) + (arrsNum_ * sizeof(float) * arrSize_)];
        memcpy(totalBytesArray, &arrsNum_, sizeof(arrsNum_));
        memcpy(totalBytesArray + sizeof(arrsNum_), &nextIndex_, sizeof(nextIndex_));
        for (int i =0 ; i < arrsNum_; i++) {
            memcpy(totalBytesArray + sizeof(arrsNum_) + sizeof(nextIndex_) + (i * sizeof(float) * arrSize_),
                   arrs_[i], sizeof(int) * arrSize_);
        }
        return totalBytesArray;
    }

    bool equals(Object *other) override {
        if (other == nullptr) return false;
        FloatMetaArray *s = dynamic_cast<FloatMetaArray*>(other);
        if (s == nullptr) return false;
        if ((this->arrsNum_ == s->arrsNum_) && (this->nextIndex_ == s->nextIndex_)) {
            for (int i =0 ; i < arrsNum_; i++) {
                if (memcmp(this->arrs_[i], s->arrs_[i], sizeof(float) * arrSize_) != 0) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
};