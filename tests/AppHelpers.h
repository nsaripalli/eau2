class Helper : public Rower{
};

class Reader : public Helper{
    bool accept(Row& row) override = 0;
};

class Writer : public Helper {
    bool accept(Row& row) override = 0;
};