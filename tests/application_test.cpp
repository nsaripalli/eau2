#include "../src/application.h"

int main() {
    Trivial t = Trivial(0, "127.0.0.2");
    t.run_();
    while(!t.done());
    return 0;
}