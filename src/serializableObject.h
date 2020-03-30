#pragma once

#include "strBuff.h"
#include "object.h"

class SerializableObject: public Object {
public:
    virtual Serialized serialize_object() = 0;
};