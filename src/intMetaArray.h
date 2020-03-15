#pragma once

#include "metaArray.h"

/*
 * IntMetaArray ::
 * int typed meta array.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class IntMetaArray : public MetaArray {
public:
    int** arrs_;

    /* delete arrs_ */
    virtual ~IntMetaArray() {
        for (int i=0; i<arrsNum_; i++)
            delete[] arrs_[i];
        delete[] arrs_;
    }

    /* Creates a new empty intMetaArray with 1 inner array */
    IntMetaArray() {
        arrs_ = new int*[1];
        arrs_[0] = new int[arrSize_]; 
        arrsNum_ = 1;
        nextIndex_ = 0;
    }

    /*
     * Adds the element to the end of the meta array (the next available index).
     * Resizes the meta array by adding another array if the meta array is at 
     * capacity (all the inner arrays are full).
     */
    void push_back(int iElem) {
        size_t arrIdx = nextIndex_ / arrSize_; // which array is the next index in
        size_t innerIdx = nextIndex_ % arrSize_; // which index is the next index at in that^ array
        
        // do we need to add a new array / are we out of room? 
        if (nextIndex_ == arrsNum_ * arrSize_) {
            int** newArrs = new int*[arrsNum_ + 1];
            for (int i=0; i<arrsNum_; i++) {
                newArrs[i] = arrs_[i];
            }
            newArrs[arrsNum_] = new int[arrSize_];
            arrsNum_ += 1;
            int** temp = arrs_;
            arrs_ = newArrs;
            delete[] temp;
        }

        arrs_[arrIdx][innerIdx] = iElem;
        nextIndex_ += 1;
    }

    /* Returns the int at the given index as if this was one normal, linear array */
    int get(size_t idx) {
        size_t arrIdx = idx / arrSize_; // which array is the index in
        size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
        return arrs_[arrIdx][innerIdx];
    }

    /* 
     * Updates the int at the given index to be the given val as if this was one 
     * normal, linear array
     */ 
    void set(size_t idx, int val) {
        if (idx == nextIndex_) 
            push_back(val);
        else {
            size_t arrIdx = idx / arrSize_; // which array is the index in
            size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
            arrs_[arrIdx][innerIdx] = val;
        }
    }
};