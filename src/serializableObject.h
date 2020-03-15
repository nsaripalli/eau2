#pragma once

#include "strBuff.h"

class SerializableObject: public Object {
public:
    virtual char* serialize_object() { return nullptr; }
};