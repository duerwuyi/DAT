#ifndef CLICKHOUSE_CLUSTER_FUZZER_HH
#define CLICKHOUSE_CLUSTER_FUZZER_HH
#include "../tester/cluster_fuzzer.hh"
#include "../feedback/action.hh"
#include "../grammar.hh"
#include "clickhouse.hh"
#define COLOCATE_NAME "colocated_key"

#define CLICKHOUSE_CLUSTER_ACTIONS "clickhouse_actions.log"
#define CLICKHOUSE_DIST_RECORD_FILE "clickhouse_setup.sql"


struct clickhouse_cluster_fuzzer : cluster_fuzzer{

    virtual void reset_init();
    // virtual bool out(std::ostream &out);
    // virtual bool check_constraint();
    // virtual void fulfil_constraint();
    clickhouse_cluster_fuzzer(shared_ptr<clickhouse_context> ctx);
};

struct on_cluster : create_dds{
    string cluster;
    virtual void out(std::ostream &out) override;
    on_cluster(shared_ptr<table> victim_table,prod *parent = nullptr);
    on_cluster(shared_ptr<table> victim_table,string c, prod *parent = nullptr)
    :create_dds(victim_table,parent){
        cluster = c;
    }
};

struct table_partitions : create_dds{
    shared_ptr<column> partition_key;
    string expr;
    virtual void out(std::ostream &out) override;
    void write_dds(string name);
    table_partitions(shared_ptr<table> victim_table, prod *parent = nullptr, string partition_key = "", string expr = "");
};

struct clickhouse_origin_table : create_dds{
    shared_ptr<create_table_stmt> origin_create_stmt;
    shared_ptr<on_cluster> cluster;
    shared_ptr<table_partitions> partitions;
    string mask_name = "";
    virtual void out(std::ostream &out) override;
    clickhouse_origin_table(shared_ptr<table> victim_table,shared_ptr<create_table_stmt> gen,
        shared_ptr<on_cluster> c, shared_ptr<table_partitions> p, prod *parent = nullptr)
    :create_dds(victim_table,parent),origin_create_stmt(gen),cluster(c),partitions(p){}
};

struct distributed_table : create_dds{
    shared_ptr<clickhouse_origin_table> origin;
    shared_ptr<column> shard_key;
    string expr;
    string db;
    string table_name;
    void write_dds(string name,string dkey = "");
    distributed_table(shared_ptr<table> victim_table, shared_ptr<clickhouse_origin_table> gen,string db,
         prod *parent = nullptr,string shard_key = "", string expr = "");
    virtual void out(std::ostream &out) override;
};

struct clickhouse_distributor : distributor
{
    shared_ptr<column> colocate_column;
    vector<string> storage_units;
    shared_ptr<on_cluster> cluster_used_for_binding;
    // shared_ptr<table_partitions> partition_used_for_binding;
    string shard_expr_for_binding;
    clickhouse_distributor(shared_ptr<Context> context): distributor(context){}
    virtual string distribute_one(shared_ptr<prod> gen, int* affect_num, int i = -1) override;
    virtual void clear_record_file() override;
};

#endif