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
    bool **arrs_;

    /* delete arrs_ */
    virtual ~BoolMetaArray() {
        for (int i = 0; i < arrsNum_; i++)
            delete[] arrs_[i];
        delete[] arrs_;
    }

    BoolMetaArray(char *totalBytesArray) : BoolMetaArray() {
        memcpy(&arrsNum_, totalBytesArray, sizeof(arrsNum_));
        memcpy(&nextIndex_, totalBytesArray + sizeof(arrsNum_), sizeof(nextIndex_));

        delete[] arrs_[0];
        delete[] arrs_;
        arrs_ = new bool *[arrsNum_];

        for (int i = 0; i < arrsNum_; i++) {
            bool *curr_arrs = new bool[arrSize_];
            arrs_[i] = curr_arrs;
            memcpy(arrs_[i],
                   totalBytesArray + sizeof(arrsNum_) + sizeof(nextIndex_) + (i * sizeof(bool) * arrSize_),
                   sizeof(bool) * arrSize_);
        }
    }

    /* Creates a new empty BoolMetaArray with 1 inner array */
    BoolMetaArray() {
        arrs_ = new bool *[1];
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
            bool **newArrs = new bool *[arrsNum_ + 1];
            for (int i = 0; i < arrsNum_; i++) {
                newArrs[i] = arrs_[i];
            }
            newArrs[arrsNum_] = new bool[arrSize_];
            arrsNum_ += 1;
            bool **temp = arrs_;
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

    Serialized serialize_object() {
        size_t size_of_output =  sizeof(arrsNum_) + sizeof(nextIndex_) + (arrsNum_ * sizeof(bool) * arrSize_);
        char *totalBytesArray = new char[size_of_output];
        memcpy(totalBytesArray, &arrsNum_, sizeof(arrsNum_));
        memcpy(totalBytesArray + sizeof(arrsNum_), &nextIndex_, sizeof(nextIndex_));
        for (int i = 0; i < arrsNum_; i++) {
            memcpy(totalBytesArray + sizeof(arrsNum_) + sizeof(nextIndex_) + (i * sizeof(bool) * arrSize_),
                   arrs_[i], sizeof(bool) * arrSize_);
        }
        struct Serialized out = {size_of_output, totalBytesArray};
        return out;
    }

    bool equals(Object *other) override {
        if (other == nullptr) return false;
        BoolMetaArray *s = dynamic_cast<BoolMetaArray *>(other);
        if (s == nullptr) return false;
        if ((this->arrsNum_ == s->arrsNum_) && (this->nextIndex_ == s->nextIndex_)) {
            for (int i = 0; i < arrsNum_; i++) {
                if (memcmp(this->arrs_[i], s->arrs_[i], sizeof(bool) * arrSize_) != 0) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
};