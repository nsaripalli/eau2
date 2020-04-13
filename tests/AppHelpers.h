#pragma once

class Helper : public Rower{
};

class Reader : public Helper{
    virtual bool accept(Row& row) override = 0;
};

class Writer : public Helper {
    virtual bool accept(Row& row) override = 0;

    virtual bool done();
};