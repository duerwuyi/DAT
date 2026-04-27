#ifndef TIDB_CLUSTER_FUZZER_HH
#define TIDB_CLUSTER_FUZZER_HH
#include "../tester/cluster_fuzzer.hh"
#include "../feedback/action.hh"
#include "../grammar.hh"
#include "tidb.hh"
#define COLOCATE_NAME "colocated_key"

struct tidb_cluster_fuzzer : cluster_fuzzer{

    virtual void reset_init();
    // virtual bool out(std::ostream &out);
    // virtual bool check_constraint();
    // virtual void fulfil_constraint();
    tidb_cluster_fuzzer(shared_ptr<tidb_context> ctx);
};

struct tidb_partition_table : create_dds{
    string method;
    shared_ptr<column> dkey;
    bool do_nothing = false;
    int shard_num = -1;
    set<int64_t> boundary;
    virtual void out(std::ostream &out) override;
    tidb_partition_table(shared_ptr<table> victim_table, prod *parent = nullptr,shared_ptr<column> d_key = nullptr,
        string strategy = "", int shard_num = -1,set<int64_t>* boundary = nullptr);
};

// TiDB table options to influence physical Region layout and write distribution.
// Example: SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2
struct tidb_shard_rowid_table : create_dds {
    // Value range is constrained by TiDB; we keep it small and valid for fuzzing.
    int shard_row_id_bits = -1;
    int pre_split_regions = -1;
    bool do_nothing = false;

    virtual void out(std::ostream &out) override;
    tidb_shard_rowid_table(shared_ptr<table> victim_table, prod *parent = nullptr,
                           int shard_row_id_bits = -1, int pre_split_regions = -1);
};

struct tidb_set_tiflash_replica : create_dds {
    int replica_count = 1;  // 0 => disable
    bool do_nothing = false;
    virtual void out(std::ostream &out) override;
    tidb_set_tiflash_replica(shared_ptr<table> victim_table, prod *parent = nullptr, int replica_count = 1);
};

struct tidb_distributor : distributor
{
    shared_ptr<column> colocate_column;
    vector<string> storage_units;
    shared_ptr<tidb_partition_table> used_for_binding;
    vector<string> binding_tables;
    tidb_distributor(shared_ptr<Context> context): distributor(context){}
    virtual string distribute_one(shared_ptr<prod> gen, int* affect_num, int i = -1) override;
    virtual void clear_record_file() override;
};

void wait_tiflash_replicas_ready(shared_ptr<tidb_context> ctx,int max_rounds, int sleep_ms);

#endif