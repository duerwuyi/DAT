#ifndef SHARDINGSPHERE_CLUSTER_FUZZER_HH
#define SHARDINGSPHERE_CLUSTER_FUZZER_HH
#include "../tester/cluster_fuzzer.hh"
#include "ss.hh"


struct ss_cluster_fuzzer : cluster_fuzzer{
    // shared_ptr<ss_context> context;

    virtual void reset_init();
    // virtual action_sequence mutate_and_run(action_sequence& victim);
    // // virtual bool out(std::ostream &out);
    // // virtual bool check_constraint();
    // // virtual void fulfil_constraint();
    ss_cluster_fuzzer(shared_ptr<ss_context> ctx);
};

// struct ss_cluster_fuzzer : cluster_fuzzer{
//     shared_ptr<ss_context> context;
//     virtual void ruin();
//     ss_cluster_fuzzer(string db, string ip, unsigned int port, shared_ptr<ss_context> ctx);
// };

#endif