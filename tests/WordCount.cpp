#include "../src/dataframe.h"
#include "../src/primatives/network.h"
#include "../src/application.h"
#include <thread>
#include <bits/stdc++.h>


struct cmp_str
{
    bool operator()(char const *a, char const *b) const
    {
        return std::strcmp(a, b) < 0;
    }
};

class Demo : public Application {
public:
    Key main = Key("main",0);
    Key c1 = Key("c1",1);
    Key c2 = Key("c2",2);
    Key c3 = Key("c3",3);

    Demo(size_t idx, const char* ip): Application(idx, ip) {}

    void run_() override {
        switch(this_node()) {
            case 0:   reader();     break;
            case 1:   counter1();      break;
            case 2:   counter2();      break;
            case 3:   counter3();      break;
            case 4:   summarizer();
        }
    }

    void reader() {
//        https://www.geeksforgeeks.org/cpp-program-read-file-word-word/
// Will read word by word and output a dataframe that is one column all all words.

        // filestream variable file
        std::fstream file;
        std::string word, t, q, filename;

        // filename of the file
        filename = "file.txt";

        // opening file
        file.open(filename.c_str());

        Schema s("S");
        DataFrame* out = new DataFrame(s);
        // extracting words from the file
        size_t curr_idx = 0;
        while (file >> word)
        {
            String* curr = new String(word.data());
            out->set(0, curr_idx, curr);
            curr_idx++;
        }

        kv->put(main, out);
    }

    void counter1() {
        DataFrame* v = kv->wait_and_get(main);
        std::map<char *, int, cmp_str> wordMap;
//        Will truncate, so this is floor of the nrows/3.
        for(size_t i=0; i< v->nrows() / 3; ++i) {
            wordMap[v->get_string(0, i)->cstr_] += 1;
        }

        Schema outSchema("SI");
        DataFrame* out = new DataFrame(outSchema);
        size_t curr_idx = 0;

        for ( const auto &p : wordMap )
        {
            out->set(0, curr_idx, p.first);
            out->set(1, curr_idx, p.second);
            curr_idx++;
        }

        kv->put(c1, out);
    }

    void counter2() {
        DataFrame* v = kv->wait_and_get(main);
        std::map<char *, int, cmp_str> wordMap;
//        Will truncate, so this is floor of the nrows/3.
        for(size_t i=v->nrows() / 3; i< (v->nrows() / 3 * 2); ++i) {
            wordMap[v->get_string(0, i)->cstr_] += 1;
        }

        Schema outSchema("SI");
        DataFrame* out = new DataFrame(outSchema);
        size_t curr_idx = 0;

        for ( const auto &p : wordMap )
        {
            out->set(0, curr_idx, p.first);
            out->set(1, curr_idx, p.second);
            curr_idx++;
        }

        kv->put(c2, out);
    }

    void counter3() {
        DataFrame* v = kv->wait_and_get(main);
        std::map<char *, int, cmp_str> wordMap;
//        Will truncate, so this is floor of the nrows/3.
        for(size_t i=(v->nrows() / 3 * 2); i< v->nrows(); ++i) {
            wordMap[v->get_string(0, i)->cstr_] += 1;
        }

        Schema outSchema("SI");
        DataFrame* out = new DataFrame(outSchema);
        size_t curr_idx = 0;

        for ( const auto &p : wordMap )
        {
            out->set(0, curr_idx, p.first);
            out->set(1, curr_idx, p.second);
            curr_idx++;
        }

        kv->put(c3, out);
    }

    void summarizer() {
        sleep(100);
        DataFrame* c1DF = kv->wait_and_get(c1);
        DataFrame* c2DF = kv->wait_and_get(c2);
        DataFrame* c3DF = kv->wait_and_get(c3);


        std::map<char *, int, cmp_str> wordMap;
        for(size_t i=0; i< c1DF->nrows() / 3; ++i) {
            wordMap[c1DF->get_string(0, i)->cstr_] += 1;
        }
        for(size_t i=0; i< c2DF->nrows() / 3; ++i) {
            wordMap[c1DF->get_string(0, i)->cstr_] += 1;
        }
        for(size_t i=0; i< c3DF->nrows() / 3; ++i) {
            wordMap[c1DF->get_string(0, i)->cstr_] += 1;
        }

//        https://stackoverflow.com/a/26282004/13221681
        for (auto const& x : wordMap)
        {
            std::cout << x.first  // string (key)
                      << ':'
                      << x.second // string's value
                      << std::endl ;
        }
    }
};

int main(int argc, char *argv[]) {

    Demo* reader = new Demo(0, "127.0.0.2");
    Demo* counter1 = new Demo(1, "127.0.0.3");
    Demo* counter2 = new Demo(2, "127.0.0.4");
    Demo* counter3 = new Demo(3, "127.0.0.5");
    Demo* summarizer = new Demo(2, "127.0.0.6");
    reader->run_();
    std::thread t1 = std::thread(&Demo::run_, counter1);
    std::thread t2 = std::thread(&Demo::run_, counter2);
    std::thread t3 = std::thread(&Demo::run_, counter3);
    std::thread t4 = std::thread(&Demo::run_, summarizer);
    sleep(5);
    while(!reader->done());
    while(!counter1->done());
    while(!counter2->done());
    while(!counter3->done());
    while(!summarizer->done());
    t1.join();
    t2.join();
    t3.join();
    t4.join();

    return 0;
}