#pragma once

#include <sys/socket.h>
#include <netinet/in.h>
#include <assert.h>
#include "string.h"
#include "strBuff.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <thread>
#include <csignal>
#include "intMetaArray.h"
#include "stringMetaArray.h"

/**
 * Useful helper for error logging / checking from socket functions
 * Credit to https://jameshfisher.com/2017/04/05/set_socket_nonblocking/ for the function
 */
int guard(int n, const char *err) {
    if (n == -1) {
        perror(err);
        exit(1);
    }
    return n;
}

/**
 * Socket ::
 * Wrapper around c socket logic / functions. Allows users
 * to create and/or connect to sockets for both server and
 * client logic. Abstracts over configuration functions and
 * structures in sockets. 
 * 
 * Sends and receives messages following our "custom" protocol:
 * Each message is sent with the amount of total bytes in the 
 * message (exclusing the first 10 characters indicating the 
 * number of bytes) and read in concatenated chunks of `packetSize`.
 * `sendAll` and `receiveAll` handles the logic for this so a
 * user does not have to.
 * 
 * All socket file descriptors from this class are non blocking. 
 * 
 * immutable
 * 
 * authors: dermer.s@husky.neu.edu & saripalli.n@northeastern.edu
 */
class Socket {
public:
    const size_t packetSize = 4096; // Total size of a packet in bytes, must be at least 1
    int sockFD; // File descriptor of this socket if it is up and listening.
    const int numLongDigits = 10; // The number of digits in a long, used for our "headers" 

    /**
     * Does nothing as the initialization logic is contingent on 
     * what the purpose of the socket is. Call create and/or connect
     * after initialization.
     */
    Socket() {
    }

    /**
     * Does nothing as there is nothing to delete.
     */
    virtual ~Socket() {
    }

    /**
     * Abstraction over socket connection logic for a TCP socket. 
     * Produces a non blocking socket file descriptor 
     * connected to *only* the ip:port given. 
     */
    int socketConnect(String *ip, int PORT) {
        struct sockaddr_in serv;
        int recFD;
        assert((recFD = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
        printf("recFD: %d\n", recFD);
        // Convert IP addresses from text to binary form
        serv.sin_family = AF_INET;
        serv.sin_port = htons(PORT);
        assert(inet_pton(AF_INET, ip->c_str(), &serv.sin_addr) > 0);
        int rv = connect(recFD,
                (struct sockaddr *) &serv,
                sizeof(serv));
        assert(rv != -1);
        fcntl(recFD, F_SETFL, O_NONBLOCK);
        return recFD;
    }

    /**
     * Abstraction over socket creation logic for a listening TCP socket. 
     * Produces a non blocking "server" socket file descriptor ready 
     * to accept connection.
     */
    int create(String *ip, int PORT) {
        struct sockaddr_in c_ain;

        sockFD = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

        // Credit to https://jameshfisher.com/2017/04/05/set_socket_nonblocking/ for the next 2 lines
        int flags = guard(fcntl(sockFD, F_GETFL), "could not get flags on TCP listening socket");
        guard(fcntl(sockFD, F_SETFL, flags | O_NONBLOCK), "could not set TCP listening socket to be non-blocking");

        bzero((char *) &c_ain, sizeof(c_ain));
        c_ain.sin_family = AF_INET;
        c_ain.sin_port = htons(PORT);

        assert(inet_pton(AF_INET, ip->c_str(), &c_ain.sin_addr) > 0);
        bind(sockFD, (struct sockaddr *) &c_ain, sizeof(c_ain));
        listen(sockFD, 5);
        return sockFD;
    }

    /**
     * Abstraction over socket accept logic for a "server" socket. 
     * NOTE: `Create` must be called first.
     * Produces a non blocking file descriptor of an incoming 
     * socket connection or -1 if no connection is incoming. 
     */
    int socketAccept() {
        struct sockaddr_in t_ain;

        int size = sizeof(t_ain);
        int newFD = accept(sockFD, (struct sockaddr *) &t_ain, (socklen_t *) &size);
        fcntl(newFD, F_SETFL, O_NONBLOCK);
        return newFD;
    }

    /**
     * Abstraction over socket writing logic with our protocol
     * described on this class. Pads the front of the message with 
     * the amount of bytes the data of the message will take up.
     * 
     * Returns the result of the write call (see man page for write). 
     */
    int sendAll(String *data, int recFD) {
        long i = data->size();
        char buf[numLongDigits];
        memset(buf, 0, numLongDigits);
        sprintf(buf, "%ld", i);

        StrBuff *prefix = new StrBuff();
        StrBuff *pad = new StrBuff();
        prefix = &prefix->c(buf);
        for (size_t i = 0; i < (size_t) numLongDigits - prefix->size_; i++) {
            pad = &pad->c("0");
        }
        pad = &pad->c(*prefix->get());


        String *output = pad->c(data->c_str(), data->size()).get();
        // printf("Out to %d: %s\n", recFD, output->c_str());
//        signal(SIGPIPE, SIG_IGN);
//        int out = send(recFD, output->c_str(), output->size(), MSG_NOSIGNAL);
        int out = write(recFD, output->c_str(), output->size());

        if (out == -1) {
            if (errno == EPIPE) {
                //it's broken
                assert(false);
            }
        }

        delete prefix;
        delete pad;
        delete output;

        return out;
    }

    /**
     * Abstraction over the socket read function, 
     * following our protocol specified for this socket class.
     * 
     * Produces a String containing all the "data" of the message.
     * Assumes the message follows the protocol; undefined behavior if not.
     */
    String *receiveAll(int recFD) {
        StrBuff fullBuffer = StrBuff();

        char buffer[packetSize];
        memset(buffer, 0, packetSize);
        int initAmount = read(recFD, buffer, numLongDigits);
        if (initAmount <= 0) {
            return new String("");
        }
        size_t totalDataToRead = 0;
        assert(1 == sscanf(buffer, "%zu", &totalDataToRead));

        while (totalDataToRead > 0) {
            memset(buffer, 0, packetSize);
            size_t toRead = std::min(totalDataToRead, packetSize);
            int amountRead = read(recFD, buffer, toRead);
            fullBuffer.c(buffer);
            totalDataToRead -= amountRead;
        }

        return fullBuffer.get();
    }

    /**
     * Closes this socket "server", stopping 
     * both receiving and transmitting.
     * 
     * Returns the resilt of the shutdown. (See man page for shutdown)
     */
    int close() {
        return shutdown(sockFD, SHUT_RDWR); // Stop both receiving and transmitting
    }
};

/**
 * NODE :: 
 * Parent class for a node in a peer-to-peer system. 
 * The node allows starting up in the foreground or background.
 * It is intended to handle nearly all of the networking logic,
 * but methods such as respondAll and Shutdown can be used with 
 * an instance of node to add behavior.
 * 
 * NOTE: Startup or bgStartup needs to always be called before any
 * other method. 
 * NOTE: Node represents an "abstract" class for shared logic, 
 * and only instances of children should exist.
 */
class Node {
public:
    Socket *sock; // owned
    String *ip; // owned
    IntMetaArray *cds = new IntMetaArray(); // owned
    int port;
    const char *name = nullptr;
    std::thread *bg; // owned
    bool done;

    String *mt = new String(""); // owned

    /**
     * Constructor for node, sets up a socket
     * bound to the given ip and port as well
     * as internal fields for networking logic.
     */
    Node(const char *_ip, int _port) {
        ip = new String(_ip);
        port = _port;
        sock = new Socket();
        sock->create(ip, port);
        bg = nullptr;
        done = false;
    }

    /**
     * Destructor for Node, deletes all owned memory
     * including the socket.
     */
    virtual ~Node() {
        delete sock;
        delete ip;
        delete cds;
        delete mt;
        delete bg;
    }

    /**
     * Starts the Node in a thread in the background.
     */
    void bgStart() {
        assert(!bg);
        bg = new std::thread(&Node::start, this);
    }

    /**
     * Starts the nodes in the foreground.
     * NOTE: Blocking
     */
    virtual void start() {
        while (!done) {
            // accepting connections
            int cd = sock->socketAccept();
            if (cd >= 0) {
                printf("[%s %s] Accepting: %d\n", name, ip->c_str(), cd);
                cds->push_back(cd);
            }
            receiveAll();
        }
    }

    /**
     * Iterates over the connected clients and reads their messages.
     * Child classes should override `onReceive_` to specify what
     * to do with these messages.
     */
    void receiveAll() {
        for (size_t i = 0; i < cds->size(); i++) {
            String *received = sock->receiveAll(cds->get(i));
            this->onReceive_(received);
            delete received;
        }
    }

    /**
     * "Protected" helper for `receiveAll` that specifies what
     * to do with received messages.
     */
    virtual void onReceive_(String *received) { exit(1); }

    /**
     * Cleanly shutdowns down the node, joining the background thread if
     * necessary.
     *
     * Should always be called in the main thread before deleting the node.
     */
    virtual void shutdown() {
        printf("[%s %s] Shutting down\n", name, ip->c_str());
        done = true;
        if (bg && bg->joinable()) {
            bg->join();
        }
        sock->close();
    }

    /**
     * Sends the given message to all connected clients,
     * "responding" to each of them.
     */
    virtual void respondAll(String *resp) {
        for (size_t j = 0; j < cds->size(); j++) {
            sock->sendAll(resp, cds->get(j));
        }
    }
};

/**
 * Server ::
 * The "server" node in a peer-to-peer system. 
 * This allows the "clients" to register their IP with the
 * server, and sends the IPS to all the registered clients.
 * 
 * Graceful shutdown is also meant to come from the server. 
 * When the server is shutdown, it tells all the registered 
 * clients to do the same.
 * 
 * NOTE: Expects only the ip addresses to be sent.
 */
class Server : public Node {
public:
    StrBuff *ips;

    /**
     * Constructor for the server, initializes the registered
     * IPS buffer and sets the name of this node type.
     */
    Server(const char *_ip, int _port) : Node(_ip, _port) {
        ips = new StrBuff();
        ips->c("IPS ");
        name = "Server";
    }

    /**
     * Deletes the IPS buffer.
     */
    virtual ~Server() {
        delete ips;
    }

    /**
     * Implemented for `receiveAll` to handle.
     * Adds the ip sent to the IPS buffer and forwards
     * it to the registered clients.
     */
    virtual void onReceive_(String *received) {
        if (!received->equals(mt)) {
            printf("[%s %s] Received: ", name, ip->c_str());
            fwrite(received->c_str(), 1, received->size(), stdout);
            printf("\n");
            fflush(stdout);
            if (cds->size() > 1) {
                ips->c(","); // token to seperate ips
            }
            ips->c(*received);
            String *ipsSoFar = ips->get();

            // sending the ips to all the clients
            printf("[%s %s] Sending IPs\n", name, ip->c_str());
            respondAll(ipsSoFar);

            delete ips;
            ips = new StrBuff();
            ips->c("IPS ");
            ips->c(&(ipsSoFar->c_str()[4]));

            delete ipsSoFar;
        }
    }

    /**
     * Tells all the clients to end,
     * then shutsdown.
     */
    virtual void shutdown() {
        String *end = new String("END");
        respondAll(end);
        Node::shutdown();
        delete end;
    }
};

/**
 * User ::
 * Helper class for clients, meant for any class that uses clients
 * and needs custom behavior for a received MSG. Should be extended
 */
class User {
public:
    virtual void use(char *msg) {}

    virtual ~User() {}
};

/**
 * Client ::
 * The "client" node in a peer-to-peer system. 
 * Registers with the "server" and is capable of sending messages
 * to other clients directly.
 * 
 * Gracefully shutsdown when told to by the server.
 * 
 * NOTE: Expects, IPS, END, or MSG headers. Anything else is undefined.
 * MSG should be used for anything not part of the internal networking
 * logic. The other headers should not be used outside of the networking
 * logic.
 */
class Client : public Node {
public:
    StringMetaArray *ips;
    User *user_;

    /**
     * Constructor for the client, initializes the registered
     * IPS list and sets the name of this node type.
     */
    Client(const char *_ip, int _port, User *user) : Node(_ip, _port) {
        ips = new StringMetaArray();
        name = "Client";
        user_ = user;
    }

    /**
     * Deletes the ip list and the ips in it.
     */
    virtual ~Client() {
        for (size_t i = 0; i < ips->size(); i++) {
            delete ips->get(i);
        }
        delete ips;
    }

    /**
     * Registers with the server then starts like normal.
     */
    virtual void start() {
        Socket serv = Socket();
        String *serverIP = new String("127.0.0.1"); // Server's "known" address
        int serverPort = 8080;
        int serverFD = serv.socketConnect(serverIP, serverPort);
        printf("[%s %s] Sending ip: %s...\n", name, ip->c_str(), ip->c_str());
        sock->sendAll(ip, serverFD);
        cds->push_back(serverFD);
        Node::start();
    }

    /**
     * Parses and deals with the possible messages received.
     * If received IPS it appends the given ip to the ip list.
     * If received MSG it simply logs the given message for now.
     * If received END it shutsdown.
     */
    virtual void onReceive_(String *received) {
        if (!received->equals(mt)) {
            printf("[Client %s] Received: %s\n", ip->c_str(), received->c_str());
            char tok[4];
            memset(&tok, 0, 4);
            tok[3] = '\0';
            strncpy(tok, received->c_str(), 3);

            if (strcmp(tok, "IPS") == 0) {
                StringMetaArray *newIps = new StringMetaArray();
                char *tok2 = strtok(&(received->c_str()[4]), ",");

                while (tok2 != nullptr) {
                    String *newIp = new String(tok2);
                    if (!newIp->equals(ip)) {
                        newIps->push_back(newIp);
                        printf("[Client %s] Adding ip: %s\n", ip->c_str(), tok2);
                    }
                    tok2 = strtok(nullptr, ",");
                }

                delete ips;
                ips = newIps;

            } else if (strcmp(tok, "MSG") == 0) {
                // print the message after "MSG "
                // printf("[Client %s] Message Received: \"%s\"\n", ip->c_str(), &(received->c_str()[4]));
                user_->use(&(received->c_str()[4]));
            } else if (strcmp(tok, "END") == 0) {
                if (bg) {
                    done = true;
                } else {
                    shutdown();
                }
            }
        }
    }

    /**
     * Sends the given message to the other known clients.
     */
    void sendMessage(String *msg) {
        for (size_t i = 0; i < ips->size(); i++) {
            Socket rec = Socket();
            printf("[%s %s] Sending msg to %s: ", name, ip->c_str(), ips->get(i)->c_str());
            fwrite(msg->c_str(), 1, msg->size(), stdout);
            fflush(stdout);
            printf("\n");
            int recFD = rec.socketConnect(ips->get(i), 8080);
            sock->sendAll(msg, recFD);
            rec.close();
        }
    }
};