#pragma once

#include <thread>

#include "column.h"
#include "fielder.h"
#include "helper.h"
#include "object.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include "strBuff.h"
#include "string.h"
#include "stringColumn.h"
#include "intColumn.h"
#include "floatColumn.h"
#include "boolColumn.h"
#include "colArray.h"
#include "network.h"
#include "string.h"
#include "dataframe.h"
#include "network.h"
#include <map>
#include <queue>
//#include "KVStore.h"

/**
 * Key :: Represents a C++ string key paired with a number to Represents
 * the index of the node the value associated with this key is stored on.
 *
 * Authors: sd & ns
 */
class Key {
public:
    std::string keyString_;
    int idx_;

    /**
     * Creates a key with the c++ string built to the given
     * key value and the index set to the given index.
     */
    Key(const char* k, int i) {
        keyString_ = std::string(k);
        idx_ = 0;
    }
};

class DataFrame;

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
        printf("WHY HERE\n");
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
    void use(char* msg);

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
            buff.c("MSG ").c(idx_).c(" ").c(key.idx_).c(" ").c("GET").c(" ").c(key.keyString_.c_str());
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
    void put(Key& k, DataFrame* df);
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A DataFrame has a schema that
 * describes it.
 */
class DataFrame : public Object {
public:

    ColumnArray *columns;
    Schema *schema; //owned

    /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
    DataFrame(DataFrame &df) : DataFrame(*df.schema) {}

    DataFrame (char* serialized) {
        size_t size_of_schema = 0;
        memcpy(&size_of_schema, serialized, sizeof(size_t));
        schema = new Schema(serialized + sizeof(size_t));
        columns = new ColumnArray(serialized + sizeof(size_t) + size_of_schema);
    }

    virtual ~DataFrame() {
        for (int i = 0; i < this->columns->length(); i++) {
            delete this->columns->get(i);
        }
        delete columns;
        delete schema;
    }

    /** Create a data frame from a schema and columns. All columns are created
      * empty. */
    DataFrame(Schema &schema) {
        this->schema =  new Schema(schema);
        this->columns = new ColumnArray();

//        Iterate through each of the elements in the row and give them to Fielder
        for (int i = 0; i < this->schema->width(); i++) {
            char curr_col_type = this->schema->col_type(i);
            if ('S' == curr_col_type) {
                this->columns->append(new StringColumn());
            }
            if ('B' == curr_col_type) {
                this->columns->append(new BoolColumn());
            }
            if ('I' == curr_col_type) {
                this->columns->append(new IntColumn());
            }
            if ('F' == curr_col_type) {
                this->columns->append(new FloatColumn());
            }
        }

    }

    /** Returns the DataFrame's schema-> Modifying the schema after a DataFrame
      * has been created in undefined. */
    Schema &get_schema() {
        return *this->schema;
    }

    /** Adds a column this DataFrame, updates the schema, the new column
      * is external, and appears as the last column of the DataFrame, the
      * name is optional and external. A nullptr colum is undefined. */
    void add_column(Column *col, String *name) {
        this->schema->add_column(col->get_type(), name);
        this->columns->append(col);
    }

    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, or request the wrong type is undefined.*/
    int get_int(size_t col, size_t row) {
        IntColumn *selectedColumn = this->columns->get(col)->as_int();
        return selectedColumn->get(row);
    }

    bool get_bool(size_t col, size_t row) {
        BoolColumn *selectedColumn = this->columns->get(col)->as_bool();
        return selectedColumn->get(row);
    }

    float get_float(size_t col, size_t row) {
        FloatColumn *selectedColumn = this->columns->get(col)->as_float();
        return selectedColumn->get(row);
    }

    String *get_string(size_t col, size_t row) {
        StringColumn *selectedColumn = this->columns->get(col)->as_string();
        return selectedColumn->get(row);
    }

    /** Set the value at the given column and row to the given value.
      * If the column is not  of the right type or the indices are out of
      * bound, the result is undefined. */
    void set(size_t col, size_t row, int val) {
        IntColumn *selectedColumn = this->columns->get(col)->as_int();
        selectedColumn->set(row, val);
    }

    void set(size_t col, size_t row, bool val) {
        BoolColumn *selectedColumn = this->columns->get(col)->as_bool();
        selectedColumn->set(row, val);
    }

    void set(size_t col, size_t row, float val) {
        FloatColumn *selectedColumn = this->columns->get(col)->as_float();
        selectedColumn->set(row, val);
    }

    void set(size_t col, size_t row, String *val) {
        StringColumn *selectedColumn = this->columns->get(col)->as_string();
        selectedColumn->set(row, val);
    }

    /** Set the fields of the given row object with values from the columns at
      * the given offset.  If the row is not form the same schema as the
      * DataFrame, results are undefined.
      */
    void fill_row(size_t idx, Row &row) {
//        std::cout << "Filling index" << std::endl;
//        std::cout << idx << std::endl;
        for (int i = 0; i < this->schema->width(); i++) {
            char curr_col_type = schema->col_type(i);
            if ('S' == curr_col_type) {
                this->set(i, idx, row.get_string(i));
            }
            if ('B' == curr_col_type) {
                this->set(i, idx, row.get_bool(i));
            }
            if ('I' == curr_col_type) {
                this->set(i, idx, row.get_int(i));
            }
            if ('F' == curr_col_type) {
                this->set(i, idx, row.get_float(i));
            }
        }
    }

    size_t find_max_len_of_columns_() {
        if (this->columns->length() == 0) {
            return 0;
        } else {
            size_t min = this->columns->get(0)->size();
            for (size_t i = 0; i < this->columns->length(); i++) {
                size_t curr_size = this->columns->get(i)->size();
                if (curr_size < min) {
                    min = curr_size;
                }
            }
            return min;
        }
    }

    /** Add a row at the end of this DataFrame. The row is expected to have
     *  the right schema and be filled with values, otherwise undedined.  */
    void add_row(Row &row) {
        size_t index_to_add_at = this->find_max_len_of_columns_();
        this->fill_row(index_to_add_at, row);
    }

    /** The number of rows in the DataFrame. */
    size_t nrows() {
        return this->find_max_len_of_columns_();
    }

    /** The number of columns in the DataFrame.*/
    size_t ncols() {
        return this->columns->length();
    }

    /** Visit rows in order */
    void map(Rower &r) {
        map_chunk_(r, 0, nrows());
    }

    /** This method clones the Rower and executes the map in parallel. Join is
     * used at the end to merge the results. */
    void pmap(Rower &r) {
        if (this->nrows() < 4) {
            map(r);
            return;
        }

        Rower *r1 = &r;
        Rower *r2 = dynamic_cast<Rower *>(r.clone());
        Rower *r3 = dynamic_cast<Rower *>(r.clone());
        Rower *r4 = dynamic_cast<Rower *>(r.clone());
        size_t chunkSize = nrows() / 4;
        size_t start = 0;
        size_t end = chunkSize;

        std::thread t1(&DataFrame::map_chunk_, this, std::ref(r), start, end);
        start = end;
        end += chunkSize;
        std::thread t2(&DataFrame::map_chunk_, this, std::ref(*r2), start, end);
        start = end;
        end += chunkSize;
        std::thread t3(&DataFrame::map_chunk_, this, std::ref(*r3), start, end);
        start = end;
        end = nrows();
        std::thread t4(&DataFrame::map_chunk_, this, std::ref(*r4), start, end);
        t1.join();
        t2.join();
        t3.join();
        t4.join();

        r1->join_delete(r2);
        r1->join_delete(r3);
        r1->join_delete(r4);
    }

    void map_chunk_(Rower &r, size_t start, size_t end) {
        for (size_t row_idx = start; row_idx < end && row_idx < this->nrows(); row_idx++) {
            Row curr_row(*this->schema);
            curr_row.set_idx(row_idx);
            for (int i = 0; i < this->schema->width(); i++) {
                char curr_col_type = schema->col_type(i);
                if ('S' == curr_col_type) {
                    curr_row.set(i, this->get_string(i, row_idx));
                }
                if ('B' == curr_col_type) {
                    curr_row.set(i, this->get_bool(i, row_idx));
                }
                if ('I' == curr_col_type) {
                    curr_row.set(i, this->get_int(i, row_idx));
                }
                if ('F' == curr_col_type) {
                    curr_row.set(i, this->get_float(i, row_idx));
                }
            }
            r.accept(curr_row);
        }
    }

    /** Create a new DataFrame, constructed from rows for which the given Rower
      * returned true from its accept method. */
    DataFrame *filter(Rower &r) {
        DataFrame *filtered_DataFrame = new DataFrame(*this->schema);
        for (int row_idx = 0; row_idx < this->nrows(); row_idx++) {
            Row *curr_row = new Row(*this->schema);
            curr_row->set_idx(row_idx);
            for (int i = 0; i < this->schema->width(); i++) {
                char curr_col_type = schema->col_type(i);
                if ('S' == curr_col_type) {
                    curr_row->set(i, this->get_string(i, row_idx));
                }
                if ('B' == curr_col_type) {
                    curr_row->set(i, this->get_bool(i, row_idx));
                }
                if ('I' == curr_col_type) {
                    curr_row->set(i, this->get_int(i, row_idx));
                }
                if ('F' == curr_col_type) {
                    curr_row->set(i, this->get_float(i, row_idx));
                }
            }
            if (r.accept(*curr_row)) {
                filtered_DataFrame->add_row(*curr_row);
            }
            delete curr_row;
        }
        return filtered_DataFrame;
    }

    /** Print the dataframe in SoR format to standard output. */
    void print() {
        for (int row_idx = 0; row_idx < this->nrows(); row_idx++) {
            Row *curr_row = new Row(*this->schema);
            curr_row->set_idx(row_idx);
            for (int i = 0; i < this->schema->width(); i++) {
                char curr_col_type = schema->col_type(i);
                if ('S' == curr_col_type) {
                    curr_row->set(i, this->get_string(i, row_idx));
                }
                if ('B' == curr_col_type) {
                    curr_row->set(i, this->get_bool(i, row_idx));
                }
                if ('I' == curr_col_type) {
                    curr_row->set(i, this->get_int(i, row_idx));
                }
                if ('F' == curr_col_type) {
                    curr_row->set(i, this->get_float(i, row_idx));
                }
            }
            Fielder *print = new PrintFielder();
            curr_row->visit(row_idx, *print);
            delete curr_row;
        }
    }

    /**
     * Takes a Float Array and creates a new one column dataframe.
    **/
    static DataFrame* fromArray(Key* key, KVStore* kv, size_t SZ, float* vals) {
        Schema *s = new Schema("F");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        kv->put(*key, df);
        return df;
    }

    /**
     * Takes an int Array and creates a new one column dataframe.
    **/
    static DataFrame* fromArray(Key* key, KVStore* kv, size_t SZ, int* vals) {
        Schema *s = new Schema("I");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        kv->put(*key, df);
        return df;
    }

    /**
     * Takes a bool Array and creates a new one column dataframe.
    **/
    static DataFrame* fromArray(Key* key, KVStore* kv, size_t SZ, bool* vals) {
        Schema *s = new Schema("B");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        kv->put(*key, df);
        return df;
    }

    /**
     * Takes a String Array and creates a new one column dataframe.
    **/
    static DataFrame* fromArray(Key* key, KVStore* kv, size_t SZ, String** vals) {
        Schema *s = new Schema("S");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        kv->put(*key, df);
        return df;
    }

    /**
     * Takes a float and creates a new one by one dataframe.
    **/
    static DataFrame* fromScalar(Key* key, KVStore* kv, float scalar) {
        Schema *s = new Schema("F");
        DataFrame* df = new DataFrame(*s);
        df->set(0, 0, scalar);
        delete s;
        kv->put(*key, df);
        return df;
    }

    char *serialize_object() {
        char* colSerialized = this->columns->serialize_object();
        char* schemaSerialized = this->schema->serialize_object();
        StrBuff internalBuffer;
        size_t schema_size = sizeof(schemaSerialized);
        internalBuffer.c(schema_size);
        internalBuffer.c(schemaSerialized);
        internalBuffer.c(colSerialized);
        char* output = internalBuffer.val_;
        internalBuffer.val_ = nullptr;
        return output;
    }
};

void KVStore::put(Key &k, DataFrame *df) {
    if (k.idx_ != idx_) {
        StrBuff buff = StrBuff();
        buff.c("MSG ").c(idx_).c(" ").c(k.idx_).c(" ").c("PUT").c(" ").c(k.keyString_.c_str()).c(" ").c(df->serialize_object());
        client_->sendMessage(buff.get());
    } else {
        map.insert(std::pair<std::string, DataFrame*>(k.keyString_, df));
    }
}

void KVStore::use(char *msg) {
    char* tok = strtok(msg, " ");
    int from = atoi(tok);
    tok = strtok(nullptr, " ");
    printf("TO: %s\n", tok);
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
        printf("HERE\n");
        tok = strtok(nullptr, " ");
        Key k = Key(tok, to);
        StrBuff buff = StrBuff();
        buff.c("MSG ").c(idx_).c(" ").c(from).c(" ").c("RES").c(" ").c(get(k)->serialize_object());
        client_->sendMessage(buff.get());
    } else if (strcmp(tok, "RES") == 0) { // df
        tok = strtok(nullptr, " ");
        *(dfq.front()) = new DataFrame(tok);
        dfq.pop();
    }
}
