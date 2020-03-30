#pragma once

#include <netinet/in.h>
#include <fcntl.h>
#include <fcntl.h>

enum class MsgKind {
    Ack = 0, Nack = 1, Put = 2,

    Reply = 3, Get = 4, WaitAndGet = 5, Status = 6,

    Kill = 7, Register = 8, Directory = 9
};


class Message : public SerializableObject {
public:

    MsgKind kind_;  // the message kind

    size_t sender_; // the index of the sender node

    size_t target_; // the index of the receiver node

    size_t id_;     // an id t unique within the node

    struct sockaddr_in c_ain{};


    Message() {
        kind_ = MsgKind::Ack;
        sender_ = 10;
        target_ = 100;
        id_ = 10;

        c_ain = {};

        bzero((char *) &c_ain, sizeof(c_ain));
        c_ain.sin_family = AF_INET;
        c_ain.sin_port = htons(2020);
    }

    Message(char *serialized) {
//        Initialise init Variables
        kind_ = MsgKind::Ack;
        sender_ = 10;
        target_ = 100;
        id_ = 10;

        c_ain = {};


        kind_ = static_cast<MsgKind>(*serialized);

        char* senderPtr = serialized + sizeof(int);
        memcpy(&sender_, senderPtr, sizeof(sender_));

        char* targetPtr = senderPtr + sizeof(sender_);
        memcpy(&target_, targetPtr, sizeof(target_));

        char* idPtr = targetPtr + sizeof(target_);
        memcpy(&id_, idPtr, sizeof(id_));

        char* cainPtr = idPtr + sizeof(id_);
        memcpy(&c_ain, cainPtr, sizeof(c_ain));


    }

    Serialized serialize_object() override {
        size_t size = sizeof(int) +
                      sizeof(sender_) +
                      sizeof(target_) +
                      sizeof(id_) +
                      sizeof(c_ain);
        char *kindptr = new char[
        size
        ];


        int kind_type = static_cast<int>(kind_);
        memcpy(kindptr, &kind_type, sizeof(int));

        char *senderptr = kindptr + sizeof(int);
        memcpy(senderptr, &sender_, sizeof(sender_));

        char *targetPtr = senderptr + sizeof(sender_);
        memcpy(targetPtr, &target_, sizeof(sender_));


        char *idPtr = targetPtr + sizeof(target_);
        memcpy(idPtr, &id_, sizeof(sender_));


        char *cainPtr = idPtr + sizeof(id_);
        memcpy(cainPtr, &c_ain, sizeof(c_ain));

        Serialized out = {size, kindptr};
        return out;
    }
};