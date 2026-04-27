#ifndef SHARDINGSPHERE_ACTION_HH
#define SHARDINGSPHERE_ACTION_HH

#include "../feedback/action.hh"
#include "../grammar.hh"
#include "ss.hh"
#define COLOCATE_NAME "colocated_key"

struct ss_create_dds : create_dds
{
    ss_create_dds(shared_ptr<table> victim_table, prod *parent = nullptr)
        : create_dds(victim_table, parent){}
    virtual void out(std::ostream &out) override {};
    virtual bool run(Context& ctx) override;
};

struct create_broadcast_table_rule : ss_create_dds
{
    string victim_table_name;
    create_broadcast_table_rule(shared_ptr<table> victim_table, prod *parent = nullptr);
    virtual void out(std::ostream &out) override;
};

//create_single_table_rule:should run on single db first
struct create_single_table_rule : ss_create_dds
{
    string victim_table_name;
    string storage_unit;
    create_single_table_rule(shared_ptr<table> victim_table, string storage_unit = "", prod *parent = nullptr);
    virtual void out(std::ostream &out) override;
    virtual void out2(std::ostream &out);
};

struct create_sharding_table_rule : ss_create_dds {
    shared_ptr<column> dkey;
    int shard_num = -1;
    bool use_colocate = false;
    string colocate;
    vector<string> storage_units;
    string strategy;
    create_sharding_table_rule(shared_ptr<table> victim_table,vector<string> storage_units, string strategy = ""
        , shared_ptr<column> dkey = nullptr, int shard_num = -1, prod *parent = nullptr);
    virtual void out(std::ostream &out) override;
};

struct create_binding_table_rule : ss_create_dds{
    vector<string> victim_names;
    create_binding_table_rule(vector<string> victim_names, prod *parent = nullptr)
        :ss_create_dds(nullptr, parent){
            this -> victim_names = victim_names;
        }
    virtual void out(std::ostream &out) override;
};

struct ss_distributor : distributor
{
    shared_ptr<column> colocate_column;
    vector<string> storage_units;
    shared_ptr<create_sharding_table_rule> used_for_binding;
    vector<string> binding_tables;
    ss_distributor(shared_ptr<Context> context): distributor(context){
        auto ctx = dynamic_pointer_cast<ss_context>(context);
        for(auto& w : ctx -> workers){
            storage_units.push_back(w.storage_unit);
        }
    }
    virtual string distribute_one(shared_ptr<prod> gen, int* affect_num, int i = -1) override;
    virtual void clear_record_file() override;
};

#endif
// #include "../feedback/differential_query_tester.hh"
// #include "ss.hh"

// struct ss_distsql_action : action {
//     string sql;
//     virtual void random_fill(Context& ctx){};
//     virtual void impact(ss_context& context){};
//     virtual void out(std::ostream &out) override{
//         out << sql << endl;
//     };
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new ss_distsql_action(*this);
//     }
// };

// struct register_storage_unit : ss_distsql_action {
//     string db;
//     string ip;
//     unsigned int port;
//     string user;
//     string password;
//     virtual void random_fill(Context& ctx) override;
//     virtual action* clone() const override {
//         return new register_storage_unit(*this);
//     }
//     virtual void impact(ss_context& context) override;
//     register_storage_unit() {};
//     register_storage_unit(shared_ptr<ss_worker_connection> c);
// };

// struct unregister_storage_unit : ss_distsql_action {
//     string ip;
//     unsigned int port;
//     virtual void random_fill(Context& ctx) override;
//     virtual action* clone() const override {
//         return new unregister_storage_unit(*this);
//     }
//     virtual void impact(ss_context& context) override;
//     unregister_storage_unit() {};
//     // unregister_storage_unit(shared_ptr<ss_worker_connection> c);
// };

// struct ss_distribute_action : ss_distsql_action {
//     shared_ptr<create_table_stmt> create_table;
//     virtual bool run(Context& ctx, int* affect_num);
//     virtual action* clone() const override {
//         return new ss_distribute_action(*this);
//     }
// };

// struct create_sharding_table_rule : ss_distribute_action {
//     virtual void random_fill(Context& ctx) override;
//     virtual action* clone() const override {
//         return new create_sharding_table_rule(*this);
//     }
// };

// struct alter_sharding_table_rule : ss_distsql_action {
//     virtual void random_fill(Context& ctx) override;
//     virtual action* clone() const override {
//         return new alter_sharding_table_rule(*this);
//     }
// };

// struct create_broadcast_table_rule : ss_distribute_action {
//     virtual void random_fill(Context& ctx) override;
//     virtual action* clone() const override {
//         return new create_broadcast_table_rule(*this);
//     }
// };

// struct load_single_table : ss_distribute_action {
//     int vicitm_id;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx, int* affect_num) override;
//     virtual action* clone() const override {
//         return new load_single_table(*this);
//     }
// };

// #endif