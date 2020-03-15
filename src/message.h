#include <netinet/in.h>

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

    sockaddr_in client;

    Message(MsgKind kind, size_t sender, size_t target, size_t id, const sockaddr_in &client) : kind_(kind),
                                                                                                sender_(sender),
                                                                                                target_(target),
                                                                                                id_(id),
                                                                                                client(client) {};

    Message(char *other) {


        struct sockaddr_in addr_cli;
    }


    char *serialize_object() override {
        int k = static_cast<int>(kind_);
    }
};