#include "../src/application.h"

int main() {
    Trivial t = Trivial(0);
    t.run_();
    while(!t.done());
    return 0;
}