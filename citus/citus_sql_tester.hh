#ifndef CITUS_SQL_TESTER_HH
#define CITUS_SQL_TESTER_HH
#include "citus.hh"
#include "citus_cluster_fuzzer.hh"
#include "citus_action.hh"
#include "../grammar.hh"

struct citus_sql_dut : citus_dut
{
    virtual void reset(void) override;
    virtual void reset_to_backup(void) override;
    virtual vector<string> get_query_plan(const vector<vector<string>>& query_plan) override;
    citus_sql_dut(string db,string ip, unsigned int port)
    : citus_dut(db, ip, port) {}
};

// struct citus_distributor : distributor
// {
//     shared_ptr<citus_context> context;
//     // virtual void distribute_all(void) override;
//     virtual string distribute_one(shared_ptr<prod> gen, int* affect_num) override;
//     citus_distributor(shared_ptr<citus_context> context);
//     virtual void clear_record_file(void) override;
// };

// struct citus_rebalancer : rebalancer
// {   
//     shared_ptr<citus_context> context;
//     virtual void rebalance_all(bool need_reset = true) override;
//     citus_rebalancer(shared_ptr<citus_context> context);
//     virtual void clear_record_file(void) override;
// };

// struct create_distributed_table : citus_action
// {
//     string victim_table_name;
//     string victim_column_name;
//     virtual void random_fill(Context& ctx);
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new create_distributed_table(*this);
//     }
// };

// struct create_reference_table : citus_action
// {
//     string victim_table_name;
//     virtual void random_fill(Context& ctx);
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new create_reference_table(*this);
//     }
// };

// struct undistribute_table : citus_action
// {
//     virtual void random_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new undistribute_table(*this);
//     }
// };

// struct alter_distributed_table : citus_action
// {
//     virtual void random_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new alter_distributed_table(*this);
//     }
// };

// struct truncate_local_data_after_distributing_table : citus_action
// {
//     virtual void random_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new truncate_local_data_after_distributing_table(*this);
//     }
// };

// struct remove_local_tables_from_metadata : citus_action{
//     virtual void random_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new remove_local_tables_from_metadata(*this);
//     }
// };

// struct citus_add_local_table_to_metadata : citus_action{
//     virtual void random_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new citus_add_local_table_to_metadata(*this);
//     }
// };

// struct update_distributed_table_colocation : citus_action{
//     virtual void random_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new update_distributed_table_colocation(*this);
//     }
// };

// //
// struct citus_move_shard_placement : citus_action{
//     virtual void random_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new citus_move_shard_placement(*this);
//     }
// };

// struct citus_rebalance : citus_action{
//     string sql0;
//     string sql1;
//     string sql2;
//     virtual void random_fill(Context& ctx);
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new citus_rebalance(*this);
//     }
// };

// struct citus_rebalance_status : citus_action{
//     virtual void random_fill(Context& ctx);
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new citus_rebalance_status(*this);
//     }
// };


// struct citus_drain_node : citus_action{
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx);
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new citus_drain_node(*this);
//     }
// };

#endif