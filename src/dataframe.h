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
#include "KVStore.h"

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

    /** Return the offset of the given column name or -1 if no such col. */
    int get_col(String &col) {
        return this->schema->col_idx(col.c_str());
    }

    /** Return the offset of the given row name or -1 if no such row. */
    int get_row(String &col) {
        return this->schema->row_idx(col.c_str());
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
        Schema *s = new Schema("f");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        return df;
    }

    /**
     * Takes an int Array and creates a new one column dataframe.
    **/
    static DataFrame* fromArray(Key* key, KVStore* kv, size_t SZ, int* vals) {
        Schema *s = new Schema("i");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        return df;
    }

    /**
     * Takes a bool Array and creates a new one column dataframe.
    **/
    static DataFrame* fromArray(Key* key, KVStore* kv, size_t SZ, bool* vals) {
        Schema *s = new Schema("b");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        return df;
    }

    /**
     * Takes a String Array and creates a new one column dataframe.
    **/
    static DataFrame* fromArray(Key* key, KVStore* kv, size_t SZ, String** vals) {
        Schema *s = new Schema("s");
        DataFrame* df = new DataFrame(*s);
        for (size_t i = 0; i < SZ; i++) {
            df->set(0, i, vals[i]);
        }
        delete s;
        return df;
    }
};