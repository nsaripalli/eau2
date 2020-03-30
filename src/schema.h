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
    CharArray *column_types;

    /** Copying constructor */
    Schema(Schema &from) {
        Schema* from_ptr = &from;
        this->column_types = new CharArray(from.column_types);
    }

    Schema(char* seriliazed) {
        this->column_types = new CharArray(seriliazed);
    }

    virtual ~Schema() {
        delete this->column_types;
    }

    /** Create an empty schema **/
    Schema() {
        this->column_types = new CharArray();
    }

    /** Create a schema from a string of types. A string that contains
      * characters other than those identifying the four type results in
      * undefined behavior. The argument is external, a nullptr argument is
      * undefined. **/
    Schema(const char *types) : Schema() {
        for (int i = 0; i < strlen(types); i++) {
            this->column_types->append(types[i]);
        }
    }

    /** Add a column of the given type and name (can be nullptr), name
      * is external. Names are expectd to be unique, duplicates result
      * in undefined behavior. */
    void add_column(char typ, String *name) {
        this->column_types->append(typ);
    }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        return this->column_types->get(idx);
    }

    /** The number of columns */
    size_t width() {
        return this->column_types->length();
    }

    char* serialize_object() {
        char* types = column_types->serialize_object();
        return types;
    }

    bool equals(Object *other) override {
        if (other == nullptr) return false;
        Schema *s = dynamic_cast<Schema*>(other);
        if (s == nullptr) return false;

        return this->column_types->equals(s->column_types);
    }
};