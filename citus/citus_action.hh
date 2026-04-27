#ifndef CITUS_ACTION_HH
#define CITUS_ACTION_HH

#include "../feedback/action.hh"
#include "../grammar.hh"
#define COLOCATE_NAME "colocated_key"

struct citus_create_dds : create_dds
{
    citus_create_dds(shared_ptr<table> victim_table, prod *parent = nullptr)
        : create_dds(victim_table, parent){}
    virtual void out(std::ostream &out) override {};
    virtual bool run(Context& ctx) override;
};

struct create_distributed_table : citus_create_dds
{
    shared_ptr<column> dkey;
    int shard_num = -1;
    bool use_colocate = false;
    string colocate;
    create_distributed_table(shared_ptr<table> victim_table, shared_ptr<column> dkey = nullptr, int shard_num = -1, prod *parent = nullptr);
    create_distributed_table(shared_ptr<table> victim_table, string colocate, shared_ptr<column> dkey = nullptr,  prod *parent = nullptr);
    virtual void out(std::ostream &out) override;
};

struct create_reference_table : citus_create_dds
{
    string victim_table_name;
    create_reference_table(shared_ptr<table> victim_table, prod *parent = nullptr);
    virtual void out(std::ostream &out) override;
};

struct citus_distributor : distributor
{
    shared_ptr<column> colocate_column;
    citus_distributor(shared_ptr<Context> context): distributor(context){}
    virtual string distribute_one(shared_ptr<prod> gen, int* affect_num, int i = -1) override;
    virtual void clear_record_file() override;
};

#endif