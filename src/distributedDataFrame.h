#pragma once

#include "dataframe.h"
#include <set>
#include <algorithm>

size_t numRowsOfEachMetaDF = 1000;

class DistributedDataFrame : public DataFrame {
public:

//    https://stackoverflow.com/a/440240/13221681
    std::string random_string( size_t length )
    {
        auto randchar = []() -> char
        {
            const char charset[] =
                    "0123456789"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "abcdefghijklmnopqrstuvwxyz";
            const size_t max_index = (sizeof(charset) - 1);
            return charset[ rand() % max_index ];
        };
        std::string str(length,0);
        std::generate_n( str.begin(), length, randchar );
        return str;
    }

    size_t numOfNodes;

    Schema *schema;
    KVStore kv;
    String *uniqueID;
    std::set<int> createdSubDFs;
    size_t maxIdx;

    DistributedDataFrame(Schema &schema, size_t numOfNodes, KVStore kvStore, String *unique_ID) {
        this->schema = new Schema(schema);
        this->numOfNodes = numOfNodes;
        this->kv = kvStore;
        this->uniqueID = unique_ID;
        this->maxIdx = 0;
    }

    DistributedDataFrame(Schema schema, size_t i, KVStore *pStore) : DistributedDataFrame(schema, i, *pStore, new String(random_string(10).c_str())) {

    }

    DistributedDataFrame(char *input) {
        Serialized raw = StrBuff::convert_back_to_original(input);
        char *serialized = raw.data;

        size_t size_of_idx = 0;
        memcpy(&size_of_idx, serialized, sizeof(size_t));
        this->maxIdx = size_of_idx;

        char *tok = serialized + sizeof(size_t);

        schema = new Schema(tok);
    }

    virtual ~DistributedDataFrame() {
        delete schema;
        delete uniqueID;
    }

    size_t getNodeIDOfRowNumber_(size_t rowNum) {
        size_t infNodeID = (rowNum / numRowsOfEachMetaDF);
        return infNodeID % numOfNodes;
    }

    size_t getDFID_(size_t rowNum) {
        return rowNum / numRowsOfEachMetaDF;
    }

    Key *getKeyForRow(size_t rowNum) {
        StrBuff buff = StrBuff();
        buff.c(*uniqueID);
        buff.c((int) this->getDFID_(rowNum));
        return new Key(buff.get(), getNodeIDOfRowNumber_(rowNum));
    }

    size_t getRowIdInSubDF(size_t rowNum) {
        return rowNum % numRowsOfEachMetaDF;
    }

    virtual Schema &get_schema() {
        return *this->schema;
    }

    DataFrame *getDataFrameWithRow(size_t row) {
        if (createdSubDFs.count(getDFID_(row)) != 0) {
            Key *key = getKeyForRow(row);
            DataFrame *df = kv.wait_and_get(*key);
            delete key;
            return df;
        } else {
            Key *key = getKeyForRow(row);
            return new DataFrame(*this->schema);
        }

    }

    void setDataFrameWithRow(size_t row, DataFrame *df) {
        createdSubDFs.insert(getDFID_(row));
        Key *key = getKeyForRow(row);
        kv.put(*key, df);
        if (row > this->maxIdx) {
            this->maxIdx = row;
        }
        delete key;
//        delete df;
    }

    void delDataFrameWithRow(size_t row, DataFrame *df) {
//        delete df;
    }

    virtual void add_column(Column *col, String *name) {
//        TODO do this
    }

    virtual int get_int(size_t col, size_t row) {
        DataFrame *df = getDataFrameWithRow(row);
        int out = df->get_int(col, getRowIdInSubDF(row));
        delDataFrameWithRow(row, df);
        return out;
    }

    virtual bool get_bool(size_t col, size_t row) {
        DataFrame *df = getDataFrameWithRow(row);
        bool out = df->get_bool(col, getRowIdInSubDF(row));
        delDataFrameWithRow(row, df);
        return out;
    }

    virtual float get_float(size_t col, size_t row) {
        DataFrame *df = getDataFrameWithRow(row);
        float out = df->get_float(col, getRowIdInSubDF(row));
        delDataFrameWithRow(row, df);
        return out;
    }

    virtual String *get_string(size_t col, size_t row) {
        DataFrame *df = getDataFrameWithRow(row);
        String *out = df->get_string(col, getRowIdInSubDF(row));
        delDataFrameWithRow(row, df);
        return out;
    }

    virtual void set(size_t col, size_t row, int val) {
        DataFrame *df = getDataFrameWithRow(row);
        df->set(col, getRowIdInSubDF(row), val);
        setDataFrameWithRow(row, df);
    }

    virtual void set(size_t col, size_t row, bool val) {
        DataFrame *df = getDataFrameWithRow(row);
        df->set(col, getRowIdInSubDF(row), val);
        setDataFrameWithRow(row, df);
    }

    virtual void set(size_t col, size_t row, float val) {
        DataFrame *df = getDataFrameWithRow(row);
        df->set(col, getRowIdInSubDF(row), val);
        setDataFrameWithRow(row, df);
    }

    virtual void set(size_t col, size_t row, String *val) {
        DataFrame *df = getDataFrameWithRow(row);
        df->set(col, getRowIdInSubDF(row), val);
        setDataFrameWithRow(row, df);
    }

    virtual void fill_row(size_t idx, Row &row) {
        DataFrame *df = getDataFrameWithRow(idx);
        df->fill_row(getRowIdInSubDF(idx), row);
        setDataFrameWithRow(idx, df);
    }

    virtual void add_row(Row &row) {
        fill_row(this->maxIdx + 1, row);
    }

    virtual size_t nrows() {
        return this->maxIdx + 1;
    }

    virtual size_t ncols() {
        return this->schema->width();
    }

    virtual void map(Rower &r) {
        map_chunk_(r, 0, nrows());
    }

    virtual void map_chunk_(Rower &r, size_t start, size_t end) {
        for (size_t i = start; i < end; i += numRowsOfEachMetaDF) {
            size_t to_idx = end - (i * numRowsOfEachMetaDF);
            DataFrame *internal = this->getDataFrameWithRow(i);
            internal->map_chunk_(r, i, to_idx);
            delDataFrameWithRow(i, internal);
        }
    }

    //        We don't currently use filter in DF anywhere so leaving this out for now
//    virtual DataFrame *filter(Rower &r) {
//    }


    virtual void print() {
        for (size_t i = 0; i < maxIdx; i += numRowsOfEachMetaDF) {
            this->getDataFrameWithRow(i)->print();
        }
    }

    virtual Serialized serialize_object() {
        // All of the data is already in the kv store, so really we only need to serialize the schema and size.

        Serialized schemaSerialized = this->schema->serialize_object();
//        Schema test(schemaSerialized);

        char max_idx_serialized[sizeof(size_t)];
        memset(&max_idx_serialized, 0, sizeof(size_t));
        memcpy(&max_idx_serialized, &this->maxIdx, sizeof(size_t));

        StrBuff internalBuffer;
        internalBuffer.c(max_idx_serialized, sizeof(size_t));
        internalBuffer.c(schemaSerialized);
        delete[] schemaSerialized.data;
        return internalBuffer.getSerialization();
    }

    virtual bool equals(Object *other) {
        if (other == nullptr) return false;
        DistributedDataFrame *o = dynamic_cast<DistributedDataFrame *>(other);
        if (o == nullptr) return false;

        if (o->maxIdx != this->maxIdx) {
            return false;
        }

        size_t num_dfs = this->maxIdx / numRowsOfEachMetaDF;
        bool is_all_equal = true;
        for (size_t i = 0; i < maxIdx; i += numRowsOfEachMetaDF) {
            is_all_equal = is_all_equal and (this->getDataFrameWithRow(i)->equals(o->getDataFrameWithRow(i)));
        }
        return is_all_equal;
    }

    void local_map(Rower r) {
        size_t start = 0;
        size_t end = nrows();
        for (size_t i = start; i < end; i += numRowsOfEachMetaDF) {
            if (getNodeIDOfRowNumber_(i) == this->kv.idx_) {
                size_t to_idx = end - (i * numRowsOfEachMetaDF);
                DataFrame *internal = this->getDataFrameWithRow(i);
                internal->map_chunk_(r, i, to_idx);
                delDataFrameWithRow(i, internal);
            }
        }
    }

    /**
  * Takes a float and creates a new one by one dataframe.
 **/
    static DataFrame *fromScalar(Key *key, KVStore *kv, size_t numOfNodes, float scalar) {
        Schema *s = new Schema("F");
        DistributedDataFrame *df = new DistributedDataFrame(*s,numOfNodes, kv);
        df->set(0, 0, scalar);
        delete s;
        kv->put(*key, df);
        return df;
    }

    /**
     * Takes a int and creates a new one by one dataframe.
    **/
    static DataFrame *fromScalar(Key *key, KVStore *kv, size_t numOfNodes, int scalar) {
        Schema *s = new Schema("I");
        DistributedDataFrame *df = new DistributedDataFrame(*s,numOfNodes, kv);
        df->set(0, 0, scalar);
        delete s;
        kv->put(*key, df);
        return df;
    }

//    TODO what am I supposed to return?
    static DataFrame* fromVisitor(Key *pKey, KVStore **pStore, const char *string, Rower writer) {
//        TODO What am I supposed to dO
    }

    static DistributedDataFrame *fromFile(const char *proj, Key &key, KVStore *pStore, size_t nodes) {
//        TODO FINISH ME
    }
};