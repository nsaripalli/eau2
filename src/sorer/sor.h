#pragma once

#include <fstream>
#include <stdio.h>
#include <iostream>
#include <vector>
#include "type.h"
#include "../dataframe.h"
#include "util.h"
#include "../distributedDataFrame.h"

using namespace std;


/**
 * Helper class for parsing sor files
 */
class Parser {
    public: 
        char ch;
        bool in_field;
        bool in_quotes;
        unsigned int bytes_read;
        unsigned int line_count;
        unsigned int current_width;
        string current_field;
        bool infer_only;
        vector<char> type_vector;
        vector<string> staging_vector;
        unsigned int length;
        DataFrame* df;

        Parser(unsigned int length_) {
            in_field = false;
            in_quotes = false;
            bytes_read = 0;
            line_count = 0;
            current_width = 0;
            current_field = "";
            length = length_;
        }

        /**
         * Basic constructor for a Parser, used when building up a DF
         * @param onlyReadSchema true if this Parser is just being using to infer schema, not write data to columns
         */
        Parser(unsigned int length, DataFrame* df_) : Parser(length) {
            infer_only = (df_ == nullptr);
            if (!infer_only) {
                vector<char> temp;
                for (int i = 0; i < df_->ncols(); i++) {
                    temp.push_back(df_->schema->col_type(i));
                }
                type_vector = temp;
            }
            df = df_;
        }

        void read_file(fstream &fin) {
            //Go character by character until the given length is reached
            while (fin >> noskipws >> ch && bytes_read < length) {
                if (!infer_only || line_count < 500) {
                    receive_character(ch);
                }
            }

            //If end of file was reached with no newline, add the staging vector
            if (!infer_only && bytes_read < length) {
                write_data();
            }
        }

        /**
         * Adds the staging vector to the columns
         */  
        void write_data() {
            for (unsigned int i = 0; i < staging_vector.size(); i++) {
                string current_field = staging_vector[i];
                //add the data contained within the field to columns
                if (current_field != "") {
                    switch(type_vector[i]) {
                        case 'B':
                            df->set(i, line_count, parse_bool(current_field));
                            break;
                        case 'F':
                            df->set(i, line_count, parse_float(current_field));
                            break;
                        case 'S':
                            df->set(i, line_count, parse_string(current_field));
                            break;
                        case 'I':
                            df->set(i, line_count, parse_int(current_field));
                            break;
                        default:
                            assert("Unrecognized type" && false);
                    }
                }
            }
        }

        /** Method to receive a character from the sor adapter
         * @param ch character being received
         */
        void receive_character(char ch) {
            bytes_read++;
            switch (ch) {
                case '\n':
                    handle_new_line();
                    break;
                case '<':
                    handle_open_tag();
                    break;
                case '>':
                    handle_close_tag();
                    break;
                case '\"':
                    handle_quote();
                    break;
                case ' ':
                    if (in_quotes) current_field = current_field + ch;
                    break;
                default:
                    if (in_field) current_field = current_field + ch;
                    break;
            }
        }
        
        /**
         * Updates the given ParseState and typeVector when a new line is encountered
         */
        void handle_new_line() {
            if (!in_quotes) {
                if (!infer_only) {
                    // add the staging vector to columns, lear the stagingVector for the next line
                    write_data();
                    staging_vector.clear();
                }
                line_count++;
                in_field = false;
                current_width = 0;
            } else {
                current_field = current_field + ch;
            }
        }

        /**
         * Updates the given ParseState when a `<` is encountered
         */
        void handle_open_tag() {
            if (in_quotes || in_field) {
                current_field = current_field + ch;
            } else if (!in_field) {
                // set the inField flag to true to indicate we are at the beginning of a new field
                in_field = true;
            }
        }

        /**
         * Updates the given ParseState and stagingVector when a `>` is encountered
         */
        void handle_close_tag() {
            if (!in_quotes) {
                if (in_field) {
                    if (type_vector.size() <= current_width) {
                        type_vector.push_back(map_to_char(get_field_type(current_field)));
                    } else {
                        type_vector.data()[current_width] = update_type(map_to_type(type_vector.at(current_width)), get_field_type(current_field));
                    }
                    current_width++;
                    in_field = false;
                    if (!infer_only) {
                        trim(current_field);
                        staging_vector.push_back(current_field);
                    }
                    current_field = "";
                }
            } else {
                current_field = current_field + ch;
            }
        }

        /**
         * Updates the type of this Column if the incomingType is less restrictive than the currentType
         * I.e. STRING replaces FLOAT replaces INT replaces BOOL
         * @param old_type The type that wa previosuly present
         * @param new_type The type that may replace this column's current Type
         */
        char update_type(Type old_type, Type new_type) {
            // check if the old column Type should be updated
            if (should_change_type(new_type, old_type)) {
                return map_to_char(old_type);
            } else {
                return map_to_char(new_type);
            }
        }

        /**
         * Updates when a quote is encountered
         */
        void handle_quote() {
            if (in_field) {
                in_quotes = !in_quotes;
                current_field = current_field + '\"';
            }
        }

        /**
         * Provides a char array representing types 
         * 
         */
        char* types() {
            char* types = new char[type_vector.size() + 1];
            for (unsigned int i = 0; i < type_vector.size(); i++) {
                types[i] = type_vector[i];
            }
            types[type_vector.size()] = '\0';
            return types;
        }

        ~Parser() {
            //Note: dont delete df, not owned.
        }
};

/**
 * Object representation of a .sor file
 * Parsing of .sor is done in the constructor of the class
 */
class SorAdapter {
    public:
        DataFrame* df_;

        /**
         * Constructor for SorAdapter class
         */
        SorAdapter(
            unsigned int from,
            unsigned int length,
            char* filename
        ) {
            string file = string(filename);
            Schema* schema = infer_schema(file, from, length);
            df_ = new DataFrame(*schema);
            build_DataFrame(file, from, length);
        }

        /**
         * Constructor for SorAdapter class that adapts to a DistributedDataFrame
         * Authors: SD & NS
         */
        SorAdapter(
            unsigned int from,
            unsigned int length,
            const char* filename,
            int numNodes,
            KVStore& kv,
            String *uid
        ) {
            string file = string(filename);
            Schema* schema = infer_schema(file, from, length);
            df_ = new DistributedDataFrame(*schema, numNodes, kv, uid);
            build_DataFrame(file, from, length);
        }

        //It will need to be implemented in future assignments. 
        Schema* infer_schema(string filename, unsigned int from, unsigned int length) {
            //create the filestream and skip to the proper location in the file
            fstream fin(filename, fstream::in);
            if (from != 0) {
                skip_to(fin, from);
            }
            // Used to keep track of the current state of parsing
            Parser* p = new Parser(length, nullptr);
            p->read_file(fin);
            char* types = p->types();
            Schema* schema = new Schema(types, true);
            delete[] types;
            delete p;
            return schema;
        }

        /**
         * Parses the .sor file at filename, loading data into columns vector
         * @param filename .sor filename to be processed
         * @param from the number of bytes to skip forward
         * @param length Number of bytes to be read
         */
        void build_DataFrame(string filename, unsigned int from, unsigned int length) {
            //create the filestream and skip to the proper location in the file
            fstream fin(filename, fstream::in);
            if (from != 0) {
                skip_to(fin, from);
            }

            // Parses the file into the DF
            Parser* p = new Parser(length, df_);
            p->read_file(fin);
            delete p;
        }

        /**
         * Skips the fstream ahead from the start to "from" bytes in,
         * and then skips to the end of the line to eliminate partial start line.
         * @param fin The file stream that will be read in
         * @param from The number of bytes to skip forward
         */
        void skip_to(fstream &fin, unsigned int from) {
            unsigned int bytesRead = 0;
            char ch;
            while (bytesRead < from && fin >> noskipws >> ch) {
                bytesRead++;
            }
            // skips to the end of the line to eliminate partial start line.
            while ((int)ch != 10 && fin >> noskipws >> ch) { }
        }

        DataFrame* get_df() {
            return df_;
        }

        ~SorAdapter() {
            //Delete df
//            delete df_;
        }
};

/**
 * Authors: SD & NS
 */
DataFrame* DataFrame::fromFile(const char* fileName, Key &key, KVStore &kv, int numNodes) {
    String* uid = new String(key.keyString_.c_str());
    SorAdapter sa = SorAdapter(0, 5000, fileName, numNodes, kv, uid);
    DataFrame* df = sa.get_df();
    kv.put(key, df);
    return df;
}
