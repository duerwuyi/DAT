#ifndef SHARDINGSPHERE_SQL_TESTER_HH
#define SHARDINGSPHERE_SQL_TESTER_HH

#include "ss_action.hh"
#include "ss_cluster_fuzzer.hh"

struct ss_sql_dut : ss_dut
{
    string single_db_name;
    virtual void reset(void) override;
    // virtual void reset_to_backup_without_data(void) ;
    virtual void reset_to_backup(void) override {};
    ss_sql_dut(string db,string ip, unsigned int port, string single_dbms_name)
    : ss_dut(db, ip, port, single_dbms_name), single_db_name(single_dbms_name) {}
    vector<string> get_query_plan(const std::vector<std::vector<std::string>>& query_plan);
};

// struct ss_distributor : distributor{
//     shared_ptr<ss_context> context;
//     virtual void distribute_all(void) override;
//     virtual std::string distribute_one(shared_ptr<prod> gen, int* affect_num) override;
//     ss_distributor(shared_ptr<ss_context> context);
//     virtual void clear_record_file(void) override;
// };

// struct ss_rebalancer : rebalancer{
//     shared_ptr<ss_context> context;
//     virtual void rebalance_all(bool need_reset = true) override;
//     ss_rebalancer(shared_ptr<ss_context> context);
//     virtual void clear_record_file(void) override;
// };

#endif
