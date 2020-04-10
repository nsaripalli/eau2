//CwC
#pragma once 

#include <regex>
#include "type.h"
#include <string>

using namespace std;

/**
 * Convenience wrapper around stoi, returns an integer from a string
 * @param s The string to be parsed
 * @return the parsed integer
 */
int parse_int(string value) {
    if (value == "") {
        return NULL;
    } else {
        return stoi(value);
    }
}

/**
 * Convenience wrapper around stod, returns a double from a string
 * @param s The string to be parsed
 * @return parsed double
 */
double parse_double(string value) {
    if (value == "") {
        return NULL;
    } else {
        return stod(value);
    }
}

/**
 * Returns bool from given string
 * @param s The string to be parsed
 * @return bool from string
 */
bool parse_bool(string value) {
    if (value == "") {
        return NULL;
    } else if (value == "1") {
        return 1;
    } else if (value == "0") {
        return 0;
    } else {
        string message = "Unable to parse boolean from string: ";
        message.append(value);
        throw message;
    }
}

/**
 * Returns String from given string
 * @param s The string to be parsed
 * @return bool from string
 */
String* parse_string(string value) {
    if (value == "") {
        return nullptr;
    } else {
        return new String(value.c_str());
    }
}

/**
 * Returns if the string is an integer
 * @param s The string to be evaluated
 * @return if s is an integer
 */
bool is_int(string value) {
    if (value.find("\"")  != string::npos) {
        return false;
    } else {
        string intPattern(R"(^(?: *)(?:\+|\-|)(\d+))");
        regex intRegex(intPattern);
        return regex_match(value, intRegex);
    }
}

/**
 * Is the string is a double
 * @param s The string to be evaluated
 * @return bool if the string is a double
 */
bool is_double(string value) {
    if (value.find("\"")  != string::npos) {
        return false;
    } else {
        string doublePattern(R"(^(?: *)(?:\+|\-|)(\d+).(\d+))");
        regex doubleRegex(doublePattern);
        return regex_match(value, doubleRegex);
    }
}

/**
 * Returns if the string is a 0 or 1, defined to be a bool by the assignment 1 spec
 * @param s The string to be evaluated
 * @return bool: if string is 1 or 0
 */
bool is_bool(string value) {
    if (value.find("\"")  != string::npos) {
        return false;
    } else {
        return value == "1" || value == "0";
    }
}

/**
 * Returns if a given value is a string
 * @param value The string to be evaluated
 * @return bool from string
 */
bool is_string(string value) {
    if (value == "") return true;
    //regex used when in quotes
    regex quotedRegex("^(\")(.*)(\")");
    //regex used when no quotes present
    regex nonQuotedRegex("^ *[^ ]+ *$");
    if (value.find("\"") != string::npos) {
      return regex_match(value, quotedRegex);
    }
    return regex_match(value, nonQuotedRegex);
}

// trim from both ends (in place)
void trim_whitespace(string &s) {
    //Trim the left
    s.erase(s.begin(), find_if(s.begin(), s.end(), [](int ch) {
        return !isspace(ch);
    }));
    //Trim the right
    s.erase(find_if(s.rbegin(), s.rend(), [](int ch) {
        return !isspace(ch);
    }).base(), s.end());
}

void trim_quotes(string &s) {
    if (s.size() >= 2 && s[0] == '\"' && s[s.size() - 1] == '\"') {
        s.erase(0, 1);
        s.erase(s.size() - 1, 1);
    }
}

void trim(string &s) {
    trim_whitespace(s);
    trim_quotes(s);
}

/**
 * Determines the most restrictive type that can be applied to a string
 *  @param fieldValue The string to be evaluated
 *  @return The most restrictive type fieldValue can represent
 */ 
Type get_field_type(string fieldValue) {
    trim_whitespace(fieldValue);
    if (is_bool(fieldValue)) return BOOL;
    if (is_int(fieldValue)) return INT;
    if (is_double(fieldValue)) return DOUBLE;
    if (is_string(fieldValue)) return STRING;
    return BOOL;
}

/**
 * Relating to the previous function, updateColumnType. True if newType is less restrictive. False otherwise
 * @param oldType The original type of a columnn
 * @param newType The new type coming in
 * @return True if the newType should override/replace the old type, false otherwise
 */
bool should_change_type(Type oldType, Type newType) {
    return newType > oldType;
}

/**
 * Maps character to Types
 * @param type The character representation on the type
 * @return the type
 */
Type map_to_type(char type) {
    switch (type) {
        case 'B':
            return BOOL;
            break;
        case 'S':
            return STRING;
            break;
        case 'D':
            return DOUBLE;
            break;
        case 'I':
            return INT;
            break;
        default:
            assert("\nUnrecognized type" && false);
            break;
    }
}

/**
 * Maps Types to character
 * @param type The Type 
 * @return the character representation of the type
 */
char map_to_char(Type type) {
    switch (type) {
        case BOOL:
            return 'B';
        case STRING:
            return 'S';
        case DOUBLE:
            return 'D';
        case INT:
            return 'I';
        default:
            assert("Unreciognized type" && false);
            break;
    }
}