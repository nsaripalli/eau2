#pragma once
#include "string.h"
#include "dataframe.h"
#include "network.h"
#include <map>
#include <queue> 

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
     * Creates a key with the c++ string built to the given
     * key value and the index set to the given index.
     */
    Key(const char* k, size_t i) {
        keyString_ = std::string(k);
        idx_ = 0;
    }
};

/**
 * KVStore :: Wrapper around a C++ map to handle a distributed key value
 * pair storage. Takes in a Key and returns the value associated with it,
 * either to local storage or by asking other nodes.
 * 
 * Authors: sd & ns
 */
class KVStore : public User {
public:
    int idx_;
    std::map<std::string, DataFrame*> map;
    Client* client_;
    std::queue<DataFrame**> dfq;

    /**
     * Default constructor, setting the KV store to be on node 0
     * with ip and port 127.0.0.2:8080
     */
    KVStore() {
        idx_ = 0;
        client_ = new Client("127.0.0.2", 8080, this);
        client_->bgStart();
    }

    /**
     * Creates a KV store associated with node `idx`
     */
    KVStore(int idx, const char* ip, int port) {
        idx_ = idx;
        client_ = new Client(ip, port, this);
        client_->bgStart();
    }

    ~KVStore() {
        client_->shutdown();
        delete client_;
    }
    
    /**
     * Parses out and deals with the three networking functionalities
     * of a KV Store:
     * 1. Adding a dataframe to the kv store
     * 2. Returning a dataframe from the store
     * 3. Accepting a returned dataframe from another store
     * 
     * All msgs match the following structure:
     * "[from idx] [to idx] [verb] [specifics... (relavant key, value info)]"
     */
    void use(char* msg) {
        char* tok = strtok(msg, " ");
        int from = atoi(tok);
        tok = strtok(nullptr, " ");
        int to = atoi(tok);
        if (to != idx_) { return; }
        tok = strtok(nullptr, " ");
        if (strcmp(tok, "PUT") == 0) { // key string, df
            tok = strtok(nullptr, " ");
            Key k = Key(tok, to);
            tok = strtok(nullptr, " ");
            DataFrame* df = new DataFrame(tok);
            put(k, df);
        } else if (strcmp(tok, "GET") == 0) { // key string
            tok = strtok(nullptr, " ");
            Key k = Key(tok, to);
            StrBuff buff = StrBuff();
            buff.c(idx_).c(" ").c(from).c(" ").c("RES").c(" ").c(get(k)->serialize_object());
            client_->sendMessage(buff.get());
        } else if (strcmp(tok, "RES") == 0) { // df
            tok = strtok(nullptr, " ");
            *(dfq.front()) = new DataFrame(tok);
            dfq.pop();
        }
    }

    /**
     * Returns the value associated with the key, 
     * NOT blocking if it needs to request the value
     * to another KV Store (node). 
     * Returns a pointer initially null that will be 
     * updated to the value when complete. 
     */
    DataFrame* get(Key& key) {
        if (key.idx_ != idx_) {
            DataFrame* df = nullptr;
            dfq.push(&df);
            StrBuff buff = StrBuff();
            buff.c(idx_).c(" ").c(key.idx_).c(" ").c("GET").c(" ").c(key.keyString_.c_str());
            client_->sendMessage(buff.get());
            return df;
        } else {
            return map.at(key.keyString_);
        }
    }

    /**
     * Returns the value associated with the key, 
     * blocking if it needs to request the value
     * to another KV Store (node)
     */
    DataFrame* wait_and_get(Key& key) {
        DataFrame* df = get(key);
        while (!df);
        return df;
    }

    /**
     * Puts the given dataframe into the KV store associated 
     * with the given key, associating the value with the given
     * key. non-blocking.
     */
    void put(Key& k, DataFrame* df) {
        if (k.idx_ != idx_) {
            StrBuff buff = StrBuff();
            buff.c(idx_).c(" ").c(k.idx_).c(" ").c("PUT").c(" ").c(k.keyString_.c_str()).c(" ").c(df->serialize_object());
            client_->sendMessage(buff.get());
        } else {
            map.insert(std::pair<std::string, DataFrame*>(k.keyString_, df));
        }
    }
};