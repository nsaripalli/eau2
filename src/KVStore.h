#pragma once
#include "string.h"
#include "dataframe.h"
#include <map>;

class DataFrame;

/**
 * Key :: Represents a C++ string key paired with a number to Represents
 * the index of the node the value associated with this key is stored on.
 * 
 * Authors: sd & ns
 */
class Key {
public:
    std::string keyString_;
    size_t idx_;

    /**
     * Creates a key with the c++ string built from the given
     * key value and the index set from the given index.
     */
    Key(const char* k, size_t i) {
        keyString_ = std::string(k);
        idx_ = 0;
    }
};

/**
 * KVStore :: Wrapper around a C++ map to handle a distributed key value
 * pair storage. Takes in a Key and returns the value associated with it,
 * either from local storage or by asking other nodes.
 * 
 * Authors: sd & ns
 */
class KVStore {
public:
    size_t idx_;
    std::map<std::string, DataFrame*> map;

    /**
     * Default constructor, setting the KV store to be on node 0.
     */
    KVStore() {
        idx_ = 0;
    }

    /**
     * Creates a KV store associated with node `idx`
     */
    KVStore(size_t idx) {
        idx_ = idx;
    }

    
    /**
     * Returns the value associated with the key, 
     * NOT blocking if it needs to request the value
     * from another KV Store (node)
     */
    DataFrame* get(Key& key) {
        if (key.idx_ != idx_) {
            // TODO
            return nullptr;
        } else {
            return map.at(key.keyString_);
        }
    }

    /**
     * Returns the value associated with the key, 
     * blocking if it needs to request the value
     * from another KV Store (node)
     */
    DataFrame* wait_and_get(Key& key) {
        if (key.idx_ != idx_) {
            // TODO
            return nullptr;
        } else {
            return get(key);
        }
    }

    /**
     * Puts the given dataframe into the KV store associated 
     * with the given key, associating the value with the given
     * key. non-blocking.
     */
    void put(Key& k, DataFrame* df) {
        if (k.idx_ != idx_) {
            // TODO
        } else {
            map.insert(std::pair<std::string, DataFrame*>(k.keyString_, df));
        }
    }
};