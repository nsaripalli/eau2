class Integer: public Object {
public:
    int inner;

    Integer(int inner) : inner(inner) {}

    int getInner() const {
        return inner;
    }

    void setInner(int in) {
        Integer::inner = in;
    }
};