#include <cstddef>
#include "../src/dataframe.h"
#include "AppHelpers.h"
#include "../src/application.h"
#include "../src/sorer/sor.h"

/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/

size_t num_nodes = 4;


/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set {
public:
    bool *vals_;  // owned; data
    size_t size_; // number of elements

    /** Creates a set of the same size as the DataFrame. */
    Set(DataFrame *df) : Set(df->nrows()) {}

    /** Creates a set of the given size. */
    Set(size_t sz) : vals_(new bool[sz]), size_(sz) {
        for (size_t i = 0; i < size_; i++)
            vals_[i] = false;
    }

    ~Set() { delete[] vals_; }

    /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
     *  values can occur if there are references to pids or uids in commits
     *  that did not appear in projects or users.
     */
    void set(size_t idx) {
        if (idx >= size_) return; // ignoring out of bound writes
        vals_[idx] = true;
    }

    /** Is idx in the set?  See comment for set(). */
    bool test(size_t idx) {
        if (idx >= size_) return true; // ignoring out of bound reads
        return vals_[idx];
    }

    size_t size() { return size_; }

    /** Performs set union in place. */
    void union_(Set &from) {
        for (size_t i = 0; i < from.size_; i++)
            if (from.test(i))
                set(i);
    }
};

/*******************************************************************************
 * A SetUpdater is a reader that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Reader {
public:
    Set &set_; // set to update

    SetUpdater(Set &set) : set_(set) {}

    /** Assume a row with at least one column of type I. Assumes that there
     * are no missing. Reads the value and sets the corresponding position.
     * The return value is irrelevant here. */
    bool accept(Row &row) {
        set_.set(row.get_int(0));
        return false;
    }

};

/*****************************************************************************
 * A SetWriter copies all the values present in the set into a one-column
 * DataFrame. The data contains all the values in the set. The DataFrame has
 * at least one integer column.
 ****************************************************************************/
class SetWriter : public Writer {
public:
    Set &set_; // set to read from
    int i_ = 0;  // position in set

    SetWriter(Set &set) : set_(set) {}

    virtual Object *clone() {
        SetWriter *sw = new SetWriter(set_);
        sw->i_ = i_;
        return sw;
    }

    void join_delete(Rower *other) {
        delete other;
    }

    /** Skip over false values and stop when the entire set has been seen */
    bool done() {
        while (i_ < set_.size_ && set_.test(i_) == false) ++i_;
        return i_ == set_.size_;
    }

    bool accept(Row &row) {
        row.set(0, i_++);
        return true;
    }
};

/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit DataFrame has the form:
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Reader {
public:
    Set &uSet; // set of collaborator
    Set &pSet; // set of projects of collaborators
    Set newProjects;  // newly tagged collaborator projects

    ProjectsTagger(Set &uSet, Set &pSet, DataFrame *proj) :
            uSet(uSet), pSet(pSet), newProjects(proj) {}

    /** The data frame must have at least two integer columns. The newProject
     * set keeps track of projects that were newly tagged (they will have to
     * be communicated to other nodes). */
    bool accept(Row &row) override {
        int pid = row.get_int(0);
        int uid = row.get_int(1);
        if (uSet.test(uid))
            if (!pSet.test(pid)) {
                pSet.set(pid);
                newProjects.set(pid);
            }
        return false;
    }
};

/***************************************************************************
 * The UserTagger is a reader that is mapped over commits, and marks all of
 * the users which commmitted to a project to which a collaborator of Linus
 * also committed as an author. The commit DataFrame has the form:
 *    pid x uid x uid
 * where the pid is the idefntifier of a project and the uids are the
 * identifiers of the author and committer.
 *************************************************************************/
class UsersTagger : public Reader {
public:
    Set &pSet;
    Set &uSet;
    Set newUsers;

    UsersTagger(Set &pSet, Set &uSet, DataFrame *users) :
            pSet(pSet), uSet(uSet), newUsers(users->nrows()) {}

    bool accept(Row &row) override {
        int pid = row.get_int(0);
        int uid = row.get_int(1);
        if (pSet.test(pid))
            if (!uSet.test(uid)) {
                uSet.set(uid);
                newUsers.set(uid);
            }
        return false;
    }
};

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
//    int DEGREES = 1;  // How many degrees of separation form linus?
    int DEGREES = 4;  // How many degrees of separation form linus?
    int LINUS = 4967;   // The uid of Linus (offset in the user df)
    const char *PROJ = "datasets/projects.ltgt";
    const char *USER = "datasets/users.ltgt";
    const char *COMM = "datasets/commits.ltgt";
    DataFrame *projects; //  pid x project name
    DataFrame *users;  // uid x user name
    DataFrame *commits;  // pid x uid x uid
    Set *uSet; // Linus' collaborators
    Set *pSet; // projects of collaborators

    Linus(size_t idx, const char *ip) : Application(idx, ip) {}
//    TODO CURRENTLY ALL KEY VALUES ARE STORED ON NODE ONE

    /** Compute DEGREES of Linus.  */
    void run_() override {
        readInput();
        for (size_t i = 0; i < DEGREES; i++) step(i);
    }

    /** Node 0 reads three files, cointainng projects, users and commits, and
     *  creates thre DistributedDataFrames. All other nodes wait and load the three
     *  DistributedDataFrames. Once we know the size of users and projects, we create
     *  sets of each (uSet and pSet). We also output a data frame with a the
     *  'tagged' users. At this point the DataFrame consists of only
     *  Linus. **/
    void readInput() {
        Key pK("projs");
        Key uK("usrs");
        Key cK("comts");
        if (this_node() == 0) {
            pln("Reading...");
            projects = DataFrame::fromFile(PROJ, pK.clone(), *kv, num_nodes);
            p("    ").p(projects->nrows()).pln(" projects");
            users = DataFrame::fromFile(USER, uK.clone(), *kv, num_nodes);
            p("    ").p(users->nrows()).pln(" users");
            commits = DataFrame::fromFile(COMM, cK.clone(), *kv, num_nodes);
            p("    ").p(commits->nrows()).pln(" commits");
            // This DataFrame contains the id of Linus.
//            delete?
            DataFrame::fromScalar(new Key("users-0-0"), kv, LINUS);
        } else {
            projects = dynamic_cast<DataFrame *>(kv->wait_and_get(pK));
            users = dynamic_cast<DataFrame *>(kv->wait_and_get(uK));
            commits = dynamic_cast<DataFrame *>(kv->wait_and_get(cK));
        }
        uSet = new Set(users);
        pSet = new Set(projects);
    }

    /** Performs a step of the linus calculation. It operates over the three
     *  datafrrames (projects, users, commits), the sets of tagged users and
     *  projects, and the users added in the previous round. */
    void step(int stage) {
        p("Stage ").pln(stage);
        // Key of the shape: users-stage-0
        Key uK(StrBuff("users-").c(stage).c("-0").get()->c_str(), stage);
        // A df with all the users added on the previous round
        DataFrame *newUsers = kv->wait_and_get(uK);
        Set delta(users);
        SetUpdater upd(delta);
        newUsers->map(upd); // all of the new users are copied to delta.
//        delete newUsers;
        ProjectsTagger ptagger(delta, *pSet, projects);
        commits->local_map(ptagger); // marking all projects touched by delta
        merge(ptagger.newProjects, "projects-", stage);
        pSet->union_(ptagger.newProjects); //
        UsersTagger utagger(ptagger.newProjects, *uSet, users);
        commits->local_map(utagger);
        merge(utagger.newUsers, "users-", stage + 1);
        uSet->union_(utagger.newUsers);
        p("    after stage ").p(stage).pln(":");
        p("        tagged projects: ").pln(pSet->size());
        p("        tagged users: ").pln(uSet->size());
    }

    /** Gather updates to the given set from all the nodes in the systems.
     * The union of those updates is then published as DataFrame.  The key
     * used for the otuput is of the form "name-stage-0" where name is either
     * 'users' or 'projects', stage is the degree of separation being
     * computed.
     */
    void merge(Set &set, char const *name, int stage) {
        if (this_node() == 0) {
            for (size_t i = 1; i < num_nodes; ++i) {
                Key nK(StrBuff(name).c(stage).c("-").c(i).get()->c_str(), i);
                DataFrame *delta = kv->wait_and_get(nK);
                p("    received delta of ").p(delta->nrows())
                        .p(" elements from node ").pln(i);
                SetUpdater upd(set);
                delta->map(upd);
//                delete delta;
            }
            p("    storing ").p(set.size()).pln(" merged elements");
            SetWriter writer(set);
            Key k(StrBuff(name).c(stage).c("-0").get()->c_str(), this->this_node());
//            delete
            DataFrame::fromVisitor(k, *kv, "I", writer);
        } else {
            p("    sending ").p(set.size()).pln(" elements to master node");
            SetWriter writer(set);
            Key k(StrBuff(name).c(stage).c("-").c(this->this_node()).get()->c_str(), this->this_node());
//            delete
            DataFrame::fromVisitor(k, *kv, "I", writer);
            Key mK(StrBuff(name).c(stage).c("-0").get()->c_str(), stage);
            DataFrame *merged = kv->wait_and_get(mK);
            p("    receiving ").p(merged->nrows()).pln(" merged elements");
            SetUpdater upd(set);
            merged->map(upd);
//            delete merged;
        }
    }
}; // Linus

int main(int argc, char *argv[]) {
// init map
    Linus *reader = new Linus(0, "127.0.0.2");
//    map1
    Linus *counter1 = new Linus(1, "127.0.0.3");
//    map2
    Linus *counter2 = new Linus(2, "127.0.0.4");
//    map3
    Linus *counter3 = new Linus(3, "127.0.0.5");
//    reducer
    Linus *summarizer = new Linus(4, "127.0.0.6");
    std::thread r = std::thread(&Linus::run_, reader);
    std::thread t1 = std::thread(&Linus::run_, counter1);
    std::thread t2 = std::thread(&Linus::run_, counter2);
    std::thread t3 = std::thread(&Linus::run_, counter3);
    std::thread t4 = std::thread(&Linus::run_, summarizer);
    sleep(5);
    while (!reader->done());
    while (!counter1->done());
    while (!counter2->done());
    while (!counter3->done());
    while (!summarizer->done());
    t1.join();
    t2.join();
    t3.join();
    t4.join();

    return 0;
}