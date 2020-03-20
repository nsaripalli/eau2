#pragma once
#include "string.h"
#include "dataframe.h"
#include <map>;

class Key {
public:
    std::string keyString_;
    size_t idx_;

    Key(const char* k, size_t i) {
        keyString_ = std::string(k);
        idx_ = 0;
    }
};

class KVStore {
public:
    size_t idx_;
    std::map<std::string, DataFrame*> map;

    KVStore(size_t idx) {
        idx_ = idx;
    }

    DataFrame* get(Key& key) {
        if (key.idx_ != idx_) {
            // TODO
        } else {
            return map.at(key.keyString_);
        }
    }

    DataFrame* wait_and_get(Key& key) {
        if (key.idx_ != idx_) {
            // TODO
        } else {
            return get(key);
        }
    }

    void put(Key& k, DataFrame* df) {
        if (k.idx_ != idx_) {
            // TODO
        } else {
            map.insert(std::pair<std::string, DataFrame*>(k.keyString_, df));
        }
    }
};