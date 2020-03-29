#pragma once

#include "metaArray.h"

/*
 * BoolMetaArray ::
 * bool typed meta array.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class BoolMetaArray : public MetaArray {
public:
    bool** arrs_;

    /* delete arrs_ */
    virtual ~BoolMetaArray() {
        for (int i=0; i<arrsNum_; i++)
            delete[] arrs_[i];
        delete[] arrs_;
    }

    /* Creates a new empty BoolMetaArray with 1 inner array */
    BoolMetaArray() {
        arrs_ = new bool*[1];
        arrs_[0] = new bool[arrSize_]; 
        arrsNum_ = 1;
        nextIndex_ = 0;
    }

    /*
     * Adds the element to the end of the meta array (the next available index).
     * Resizes the meta array by adding another array if the meta array is at 
     * capacity (all the inner arrays are full).
     */
    void push_back(bool bElem) {
        size_t arrIdx = nextIndex_ / arrSize_; // which array is the next index in
        size_t innerIdx = nextIndex_ % arrSize_; // which index is the next index at in that^ array
        
        // do we need to add a new array / are we out of room? 
        if (nextIndex_ == arrsNum_ * arrSize_) {
            bool** newArrs = new bool*[arrsNum_ + 1];
            for (int i=0; i<arrsNum_; i++) {
                newArrs[i] = arrs_[i];
            }
            newArrs[arrsNum_] = new bool[arrSize_];
            arrsNum_ += 1;
            bool** temp = arrs_;
            arrs_ = newArrs;
            delete[] temp;
        }

        arrs_[arrIdx][innerIdx] = bElem;
        nextIndex_ += 1;
    }

    /* Returns the bool at the given index as if this was one normal, linear array */
    bool get(size_t idx) {
        size_t arrIdx = idx / arrSize_; // which array is the index in
        size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
        return arrs_[arrIdx][innerIdx];
    }

    /* 
     * Updates the bool at the given index to be the given val as if this was one 
     * normal, linear array
     */ 
    void set(size_t idx, bool val) {
        if (idx == nextIndex_) 
            push_back(val);
        else {
            size_t arrIdx = idx / arrSize_; // which array is the index in
            size_t innerIdx = idx % arrSize_; // which index is the index at in that^ array
            arrs_[arrIdx][innerIdx] = val;
        }
    }

    char *serialize_object() {
        char* totalBytesArray = new char[sizeof(arrsNum_) + sizeof(nextIndex_) + (arrsNum_ * sizeof(bool) * arrSize_)];
        memcpy(totalBytesArray, &arrsNum_, sizeof(arrsNum_));
        memcpy(totalBytesArray + sizeof(arrsNum_), &nextIndex_, sizeof(nextIndex_));
        for (int i =0 ; i < arrsNum_; i++) {
            memcpy(totalBytesArray + sizeof(arrsNum_) + sizeof(nextIndex_) + (i * sizeof(bool) * arrSize_),
                   arrs_[i], sizeof(int) * arrSize_);
        }
        return totalBytesArray;
    }
};