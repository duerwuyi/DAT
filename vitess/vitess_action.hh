#ifndef VITESS_ACTION_HH
#define VITESS_ACTION_HH
#include "../feedback/action.hh"
#include "../grammar.hh"
#include "vitess.hh"
#define COLOCATE_NAME "colocated_key"

struct vitess_distributor : distributor{
    // // distributed cluster
    // vschema dist_sharded_vschema_;
    // bool dist_sharded_vschema_inited_ = false;

    // // single cluster
    // vschema single_local_vschema_;
    // bool single_local_vschema_inited_ = false;

    // pinned local  keyspace id local table  shard
    std::string pinned_local_kid_ = "10";   // 0x10  -40 shard

    // void ensure_vschemas_inited();

    shared_ptr<column> colocate_column;
    string colocate_vindex;

    std::map<std::string, pair<string,int>> vtctldclient_config;
    vtctldclient vitess_sharded = vtctldclient("host.docker.internal", 25999);
    vtctldclient vitess_single = vtctldclient("host.docker.internal", 15999);
    vschema v_distributed;
    vschema v_dist_local;
    vschema v_single;
    vitess_distributor(shared_ptr<Context> context);
    virtual string distribute_one(shared_ptr<prod> gen, int* affect_num, int i = -1) override;
    virtual void clear_record_file() override;
};

#endif