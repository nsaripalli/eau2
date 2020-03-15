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

    Message messageTesting;
    char* serialized = messageTesting.serialize_object();
    Message copyTesting(serialized);

    assert(messageTesting.target_ == copyTesting.target_);
    assert(messageTesting.kind_ == copyTesting.kind_);
    assert(messageTesting.id_ == copyTesting.id_);
    assert(messageTesting.sender_ == copyTesting.sender_);

    IntArray itesting;
    for (int i = 0; i < 11; i++) {
        itesting.append(1);
    }

    char* testSerialization = itesting.serialize_object();
    IntArray deSeralized(testSerialization);

    assert(itesting.equals(&deSeralized));

}