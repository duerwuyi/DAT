#include "vitess_cluster.hh"

vitess_cluster_fuzzer::vitess_cluster_fuzzer(shared_ptr<vitess_context> ctx)
: cluster_fuzzer(ctx, VITESS_SAVED + string(VITESS_CLUSTER_ACTIONS)){
    base_context = ctx;
    auto context = dynamic_pointer_cast<vitess_context>(base_context);
    context -> logfile = VITESS_SAVED + string(VITESS_CLUSTER_ACTIONS);
}

void vitess_cluster_fuzzer::reset_init(){
    auto context = dynamic_pointer_cast<vitess_context>(base_context);
    context -> master_dut ->reset();
}