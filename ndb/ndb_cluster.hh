#ifndef NDB_CLUSTER_HH
#define NDB_CLUSTER_HH
#include "../tester/cluster_fuzzer.hh"
#include "ndb.hh"
#include "../feedback/action.hh"
#include "../grammar.hh"
#define NDB_SAVED "NDB/saved"
#define NDB_CLUSTER_ACTIONS "NDB_cluster_actions.log"

struct ndb_cluster_fuzzer : cluster_fuzzer{
    // shared_ptr<ss_context> context;

    virtual void reset_init() ;
    // virtual action_sequence mutate_and_run(action_sequence& victim);
    // // virtual bool out(std::ostream &out);
    // // virtual bool check_constraint();
    // // virtual void fulfil_constraint();
    ndb_cluster_fuzzer(shared_ptr<ndb_context> ctx);
};

struct ndb_create_table : create_dds{
    shared_ptr<create_table_stmt> origin_stmt;
    string engine;
    bool fully_replicated = false;
    int pcount;
    int partition_key_index=-1;
    virtual void out(std::ostream &out);
    void assign_a_partition_key(string name = "");
    ndb_create_table(shared_ptr<create_table_stmt> stmt, prod* p)
    :create_dds(stmt->created_table, p)
    {origin_stmt = stmt;}
};

struct ndb_distributor : distributor{
    shared_ptr<column> colocate_column;
    int colocated_pcount;

    ndb_distributor(shared_ptr<Context> context): distributor(context){};
    virtual string distribute_one(shared_ptr<prod> gen, int* affect_num, int i = -1) override;
    virtual void clear_record_file() override;
};

#endif