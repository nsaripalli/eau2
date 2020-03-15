#pragma once

#include "stringArray.h"
#include "string.h"
#include "charArray.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
public:
    StringArray *column_names;
    CharArray *column_types;
    StringArray *row_names;

    /** Copying constructor */
//    NOTE: This does not copy over row names
    Schema(Schema &from) {
        Schema* from_ptr = &from;
        this->column_names = new StringArray(from.column_names);
        this->column_types = new CharArray(from.column_types);
        this->row_names = new StringArray();
    }

    virtual ~Schema() {
        delete this->column_names;
        delete this->column_types;
        delete this->row_names;
    }

    /** Create an empty schema **/
    Schema() {
        this->column_names = new StringArray();
        this->column_types = new CharArray();
        this->row_names = new StringArray();
    }

    /** Create a schema from a string of types. A string that contains
      * characters other than those identifying the four type results in
      * undefined behavior. The argument is external, a nullptr argument is
      * undefined. **/
    Schema(const char *types) : Schema() {
        for (int i = 0; i < strlen(types); i++) {
            this->column_types->append(types[i]);
            this->column_names->append(nullptr);
        }
    }

    /** Add a column of the given type and name (can be nullptr), name
      * is external. Names are expectd to be unique, duplicates result
      * in undefined behavior. */
    void add_column(char typ, String *name) {
        this->column_types->append(typ);
        this->column_names->append(name);
    }

    /** Add a row with a name (possibly nullptr), name is external.  Names are
     *  expectd to be unique, duplicates result in undefined behavior. */
    void add_row(String *name) {
        this->row_names->append(name);
    }

    /** Return name of row at idx; nullptr indicates no name. An idx >= width
      * is undefined. */
    String *row_name(size_t idx) {
        return this->row_names->get(idx);
    }

    /** Return name of column at idx; nullptr indicates no name given.
      *  An idx >= width is undefined.*/
    String *col_name(size_t idx) {
        return this->column_names->get(idx);
    }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        return this->column_types->get(idx);
    }

    /** Given a column name return its index, or -1. */
    int col_idx(const char *name) {
        String* wrapped_name = new String(name);
        int output = this->column_names->index_of(wrapped_name);
        delete wrapped_name;
        return output;
    }

    /** Given a row name return its index, or -1. */
    int row_idx(const char *name) {
        String* wrapped_name = new String(name);
        int output = this->row_names->index_of(wrapped_name);
        delete wrapped_name;
        return output;
    }

    /** The number of columns */
    size_t width() {
        return this->column_types->length();
    }

    /** The number of rows */
    size_t length() {
        return this->row_names->length();
    }
};