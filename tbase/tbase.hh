#ifndef TBASE_HH
#define TBASE_HH
#include "../postgres.hh"
#include "../grammar.hh"
#define DB_RECORD_FILE "db_setup.sql"
#define TBASE_SAVING_DIR "tbase/saved/"
#define TBASE_CONFIGURATION_FILE "tbase/tbase_config"
#define TBASE_CLUSTER_ACTIONS "cluster_actions.sql"
#define TBASE_DIST_RECORD_FILE "tbase_setup.sql"
#define TBASE_DIST_ALTER_FILE "tbase_alter.sql"
#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

struct tbase_dut : dut_base{
    shared_ptr<dut_libpq> postgres_dut;

    virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL) ;
    virtual void reset(void);
    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    tbase_dut(string db,string ip, unsigned int port);
};

struct schema_tbase : schema_pqxx{
    shared_ptr<tbase_dut> dut;
    vector<string> node_names;
    map<string, vector<string>> groups;
    void refresh(void);
    schema_tbase(string db, string ip, unsigned int port, bool no_catalog);
};

struct tbase_context : Context{
    std::shared_ptr<tbase_dut> master_dut;
    shared_ptr<schema_tbase> tbase_schema;
    shared_ptr<create_table_stmt> temp_table;
};

// struct tbase_action : action{
//     string sql;
//     virtual void random_fill(Context& ctx){};
//     virtual void out(std::ostream &out) override{
//         out << sql;
//     };
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new tbase_action(*this);
//     }
// };

// struct create_node_group : tbase_action{
//     string group_name;
//     vector<string> node_names;
//     virtual void random_fill(Context& ctx) override;
//     virtual void default_fill(Context& ctx);
//     virtual action* clone() const override {
//         return new create_node_group(*this);
//     }
// };

// struct tbase_cluster_config_fuzzer : cluster_fuzzer_base{
//     shared_ptr<tbase_context> context;

//     virtual void reset_init();
//     virtual action_sequence mutate_and_run(action_sequence& victim);
//     tbase_cluster_config_fuzzer(string db, string ip, unsigned int port, shared_ptr<tbase_context> ctx);
// };

// struct tbase_cluster_fuzzer : cluster_fuzzer{
//     shared_ptr<tbase_context> context;
//     virtual void ruin();
//     tbase_cluster_fuzzer(string db, string ip, unsigned int port, shared_ptr<tbase_context> ctx);
// };

// // tbase does not have rebalancer
// struct tbase_rebalancer : rebalancer
// {   
//     shared_ptr<tbase_context> context;
//     virtual void rebalance_all(bool need_reset = true) override{};
//     tbase_rebalancer(shared_ptr<tbase_context> context){};
//     virtual void clear_record_file(void) override{};
// };

#endif
