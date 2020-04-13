#pragma once

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
public:

    /** Called before visiting a row, the argument is the row offset in the
      dataframe. */
    virtual void start(size_t r) {};

    /** Called for fields of the argument's type with the value of the field. */
    virtual void accept(bool b) {};
    virtual void accept(float f) {};
    virtual void accept(int i) {};
    virtual void accept(String* s) {};

    /** Called when all fields have been seen. */
    virtual void done() {};
};

class PrintFielder : public Fielder {
public:
    void start(size_t r) override {
    }

    void accept(bool b) override {
        std::cout << b;
    }

    void accept(float f) override {
        std::cout << f;
    }

    void accept(int i) override {
        std::cout << i;
    }

    void accept(String *s) override {
        std::cout << s;
    }

    void done() override {
        std::cout << std::endl;
    }
};