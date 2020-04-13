#pragma once

#include "../primatives/object.h"

/*
 * MetaArray ::
 * Represents an abstraction of arrays such that it 
 * can be resized without any copying of "payloads".
 * This resize happens linearly (one new "inner" array
 * whenever capacity is reached) because "previous 
 * projects had trouble scaling, in particular due to 
 * doubling of array sizes when they need to grow"
 * as per the spec.
 * Only the typed child classes should be used.
 * Implemented as a resizable array of arrays.
 * MetaArrays do not take ownership of any data passed in.
 * MetaArrays are mutable, equality is pointer equality. 
 * 
 * The size of the inner arrays can be configured here. 
 * It can be reduced for testing, to allow for more
 * resizing and thus smaller tests that still test
 * full functionality. Larger tests (such as with
 * loops) would avoid needing to change this value.
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class MetaArray : public SerializableObject {
public:
    static const size_t arrSize_ = 100; // (constant) size of each array, make smaller for testing to be easier
    size_t arrsNum_; // number of arrays
    size_t nextIndex_; // the next open index, a new array should be added if nextIndex_ = arrsNum_ * arrSize_

    /* Overrides hash_me to match pointer equality */
    virtual size_t hash_me() { return reinterpret_cast<size_t>(this); };

    virtual size_t size() { return nextIndex_; }

    virtual Serialized serialize_object() = 0;
};