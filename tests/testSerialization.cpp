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

int testboolMetaArray() {
    BoolMetaArray original;
    for (int i = 0; i < 100; i++) {
        original.push_back(i % 1);
    }

    char *serialized = original.serialize_object().data;

    BoolMetaArray dup(serialized);

    assert(original.equals(&dup));

    delete []serialized;

    return 0;
}

int testFloatMetaArray() {
    FloatMetaArray original;
    for (int i = 0; i < 100; i++) {
        original.push_back(i);
    }

    char *serialized = original.serialize_object().data;

    FloatMetaArray dup(serialized);

    assert(original.equals(&dup));
    delete[] serialized;
    return 0;
}

int testIntMetaArray() {
    IntMetaArray original;
    for (int i = 0; i < 100; i++) {
        original.push_back(i);
    }

    char *serialized = original.serialize_object().data;

    IntMetaArray dup(serialized);

    assert(original.equals(&dup));

    delete[] serialized;

    return 0;
}

int testStringMetaArray() {
    StringMetaArray original;
    String test("test");
    for (int i = 0; i < 100; i++) {
        original.push_back(&test);
    }

    char* serialized = original.serialize_object().data;

    StringMetaArray dup(serialized);
    assert(original.equals(&dup));

    delete[] serialized;
    return 0;
}

int testStringArray() {
    StringArray original;
    String test("test");
    for (int i = 0; i < 100; i++) {
        original.append(&test);
    }

    char* serialized = original.serialize_object().data;

    StringArray dup(serialized);
    assert(original.equals(&dup));

    delete[] serialized;
    return 0;
}

int testMetaArray() {
    testboolMetaArray();
    testIntMetaArray();
    testFloatMetaArray();
    testStringMetaArray();
    return 0;
}

int testSchema() {
    Schema original("BIFSSSSSBBIIFFF");
    char *ser = original.serialize_object().data;
    Schema dup(ser);

    assert(original.equals(&dup));

    delete[] ser;
    return 0;

}

int testCharArray() {
    CharArray orig;
    const char *random = "09840jndfkldzuu r DqJD MLKXZ";
    for (int i = 0; i < 100; i++) {
        orig.append(random[i % 27]);
    }
    char *seri = orig.serialize_object().data;

    CharArray dump(seri);

    assert(orig.equals(&dump));
    delete[] seri;
    return 0;
}

int testIntCol() {
    IntColumn original;
    for (int i = 0; i < 100; i++) {
        original.push_back(i);
    }

    char *serialized = original.serialize_object().data;

    IntColumn dup(serialized);

    assert(original.equals(&dup));
    delete[] serialized;
    return 0;
}

int testBoolCol() {
    BoolColumn original;
    for (int i = 0; i < 100; i++) {
        original.push_back(i % 1);
    }

    char *serialized = original.serialize_object().data;

    BoolColumn dup(serialized);

    assert(original.equals(&dup));
    delete[] serialized;
    return 0;
}

int testColumnArray() {
    ColumnArray originalColArray;

    IntColumn originalIntColumn;
    for (int i = 0; i < 100; i++) {
        originalIntColumn.push_back(i);
    }

    BoolColumn originalBoolColumn;
    for (int i = 0; i < 100; i++) {
        originalBoolColumn.push_back(i % 1);
    }

    originalColArray.append(&originalIntColumn);
    originalColArray.append(&originalBoolColumn);

    Serialized s = originalColArray.serialize_object();
    char* serialized = s.data;
    const char* schema = "IB";
    ColumnArray dupColArray(serialized, (char*) schema);
    assert(originalColArray.equals(&dupColArray));

//    delete schema;
    delete[] serialized;

    return 0;
}

int testIntDF() {
    Schema s("I");
    DataFrame testingDF(s);
    testingDF.set(0, 0, 0);
    for (int i = 1; i < 17; i++) {
        testingDF.set(0, i, i);
    }
    testingDF.set(0, 17, 9999);

    Serialized testSerializationDF = testingDF.serialize_object();

    DataFrame deSeralizedDF(testSerializationDF.data);


    assert(testingDF.equals(&deSeralizedDF));
    delete[] testSerializationDF.data;
    return 0;
}


int testLargeIntZerosDF() {
    Schema s("I");
    DataFrame testingDF(s);
    testingDF.set(0, 0, 0);
    for (int i = 1; i < 100*1000; i++) {
        testingDF.set(0, i, 0);
    }

    Serialized testSerializationDF = testingDF.serialize_object();

    DataFrame deSeralizedDF(testSerializationDF.data);


    assert(testingDF.equals(&deSeralizedDF));
    delete[] testSerializationDF.data;
    return 0;
}

int testLargeIntDF() {
    testLargeIntZerosDF();
    Schema s("I");
    DataFrame testingDF(s);
    testingDF.set(0, 0, 0);
    for (int i = 1; i < 100*1000; i++) {
        testingDF.set(0, i, i);
    }
    testingDF.set(0, 17, 9999);

    Serialized testSerializationDF = testingDF.serialize_object();

    DataFrame deSeralizedDF(testSerializationDF.data);


    assert(testingDF.equals(&deSeralizedDF));
    delete[] testSerializationDF.data;
    return 0;
}


int testDF() {
    testIntDF();
    testLargeIntDF();
    Schema s("B");
    DataFrame testingDF(s);
    testingDF.set(0, 0, false);
    for (int i = 1; i < 17; i++) {
        testingDF.set(0, i, true);
    }
    testingDF.set(0, 17, false);

    Serialized testSerializationDF = testingDF.serialize_object();

    DataFrame deSeralizedDF(testSerializationDF.data);


    assert(testingDF.equals(&deSeralizedDF));
    delete[] testSerializationDF.data;
    return 0;
}

int testRegularArrays() {
    BoolArray testing;
    for (int i = 0; i < 11; i++) {
        testing.append(true);
    }

    char* testSerialization = testing.serialize_object().data;
    BoolArray deSeralized(testSerialization);

    assert(testing.equals(&deSeralized));

    Message messageTesting;
    char* serialized = messageTesting.serialize_object().data;
    Message copyTesting(serialized);

    assert(messageTesting.target_ == copyTesting.target_);
    assert(messageTesting.kind_ == copyTesting.kind_);
    assert(messageTesting.id_ == copyTesting.id_);
    assert(messageTesting.sender_ == copyTesting.sender_);

    IntArray itesting;
    for (int i = 0; i < 11; i++) {
        itesting.append(1);
    }

    char* testSerializationI = itesting.serialize_object();
    IntArray deSeralizedI(testSerializationI);

    assert(itesting.equals(&deSeralizedI));

    delete testSerializationI;

    return 0;
}

int main() {
    testRegularArrays();
    testMetaArray();
    testSchema();
    testCharArray();
    testIntCol();
    testBoolCol();
    testColumnArray();
    testDF();
    testStringArray();
}
