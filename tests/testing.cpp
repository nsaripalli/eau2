#include "../src/boolArray.h"
#include "../src/floatArray.h"
#include "../src/helper.h"
#include "../src/intArray.h"
#include "../src/object.h"
#include "../src/strBuff.h"
#include "../src/string.h"
#include "../src/stringArray.h"
#include "../src/message.h"
#include "../src/dataframe.h"

int main() {
//    BoolArray testing;
//    for (int i = 0; i < 11; i++) {
//        testing.append(true);
//    }
//
//    char* testSerialization = testing.serialize_object();
//    BoolArray deSeralized(testSerialization);
//
//    assert(testing.equals(&deSeralized));
//
//    Message messageTesting;
//    char* serialized = messageTesting.serialize_object();
//    Message copyTesting(serialized);
//
//    assert(messageTesting.target_ == copyTesting.target_);
//    assert(messageTesting.kind_ == copyTesting.kind_);
//    assert(messageTesting.id_ == copyTesting.id_);
//    assert(messageTesting.sender_ == copyTesting.sender_);
//
//    IntArray itesting;
//    for (int i = 0; i < 11; i++) {
//        itesting.append(1);
//    }
//
//    char* testSerializationI = itesting.serialize_object();
//    IntArray deSeralizedI(testSerializationI);
//
//    assert(itesting.equals(&deSeralizedI));
//
//    delete testSerializationI;
//
    Schema s("B");
    DataFrame testingDF(s);
    testingDF.set(0, 0, false);
    for (int i = 1; i < 17; i++) {
        testingDF.set(0, i, true);
    }
    testingDF.set(0, 17, false);

    char* testSerializationDF = testingDF.serialize_object();
    DataFrame deSeralizedDF(testSerializationDF);


    assert(testingDF.equals(&deSeralizedDF));
    delete[] testSerializationDF;
}