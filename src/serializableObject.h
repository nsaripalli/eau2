#pragma once

#include "strBuff.h"

class SerializableObject: public Object {
public:
    virtual Serialized serialize_object() = 0;
};