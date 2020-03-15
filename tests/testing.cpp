#include "../src/boolArray.h"
#include "../src/floatArray.h"
#include "../src/helper.h"
#include "../src/intArray.h"
#include "../src/object.h"
#include "../src/strBuff.h"
#include "../src/string.h"
#include "../src/stringArray.h"
#include "../src/message.h"

int main() {
    BoolArray testing;
    for (int i = 0; i < 11; i++) {
        testing.append(true);
    }

    char* testSerialization = testing.serialize_object();
    BoolArray deSeralized(testSerialization);

    assert(testing.equals(&deSeralized));

}