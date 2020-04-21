#pragma once

#include <assert.h>
#include "string.h"
#include "serializableObject.h"
#include "object.h"


/** A string buffer builds a string from various pieces.
 *  author: jv */
class StrBuff : public Object {
public:

    StrBuff(const char *string): StrBuff() {
        this->c(string);
    }

    char *val_; // owned; consumed by get()
    size_t capacity_;
    size_t size_;

    StrBuff() {
        val_ = new char[capacity_ = 10];
        size_ = 0;
    }

    virtual ~StrBuff() {}

    void grow_by_(size_t step) {
        if (step + size_ < capacity_) return;
        capacity_ *= 2;
        if (step + size_ >= capacity_) capacity_ += step;
        char *oldV = val_;
        val_ = new char[capacity_];
        memcpy(val_, oldV, size_);
        delete[] oldV;
    }

    StrBuff &c(const char *str) {
        size_t step = strlen(str);
        return c(str, step);
    }

    StrBuff &c(const char *str, size_t step) {
        grow_by_(step);
        memcpy(val_ + size_, str, step);
        size_ += step;
        return *this;
    }

    StrBuff &cc(const char str) {
        return this->c(&str, sizeof(char));
    }

    StrBuff &c(String &s) { return c(s.c_str()); }

    StrBuff &c(size_t v) {
        std::string tmp = std::to_string(v);
        const char*  str = (tmp.c_str());
        size_t step = strlen(str);
        return c(str, step);
    } // Cpp

    StrBuff &c(int v) {
        std::string tmp = std::to_string(v);
        const char*  str = (tmp.c_str());
        size_t step = strlen(str);
        return c(str, step);
    } // Cpp

    StrBuff c(Serialized s) {
        return c(s.data, s.size);
    }

    String *get() {
        assert(val_ != nullptr); // can be called only once
        grow_by_(1);     // ensure space for terminator
        val_[size_] = 0; // terminate
        String *res = new String(true, val_, size_);
        val_ = nullptr; // val_ was consumed above
        return res;
    }

    Serialized getSerialization() {
        Serialized out = {this->size_, this->val_};
        this->val_ = nullptr; // val_ was consumed above
        return out;
    }

    Serialized static convert_to_escaped(Serialized input) {
        StrBuff output;
        for (size_t i = 0; i < input.size; i++) {
            switch (input.data[i]) {
                case '\0':
                    output.c("|9", sizeof(char) * 2);
                    break;
                case '|':
                    output.c("||", sizeof(char) * 2);
                    break;
                default:
                    output.cc(input.data[i]);
                    break;
            }
        }
        output.cc('\0');
        return output.getSerialization();
    }

    Serialized static convert_back_to_original(char *input) {
        StrBuff output;
        size_t curr_idx = 0;
        while (input[curr_idx] != '\0') {
            switch (input[curr_idx]) {
                case '|':
                    if (input[curr_idx + 1] == '|') {
                        output.cc('|');
                        curr_idx++;
                    } else if (input[curr_idx + 1] == '9') {
                        output.cc('\0');
                        curr_idx++;
                    }
                    break;
                case '\0':
//                    We should never be here.
                    assert(false);
                    break;
                default:
                    output.cc(input[curr_idx]);
                    break;
            }
            curr_idx++;
        }
//        output.cc('\0');
        return output.getSerialization();
    }
};
