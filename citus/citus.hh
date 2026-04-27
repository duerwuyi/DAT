#ifndef CITUS_HH
#define CITUS_HH
#include "../postgres.hh"

#include "../context/context.hh"
#define CITUS_SAVING_DIR "citus/saved/"
#define CITUS_CONFIGURATION_FILE "citus/citus_config"
#define CITUS_CLUSTER_ACTIONS "cluster_actions.sql"
#define CITUS_DIST_RECORD_FILE "citus_setup.sql"
#define CITUS_DIST_ALTER_FILE "citus_alter.sql"
#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

struct citus_dut : dut_base{
    shared_ptr<dut_libpq> postgres_dut;

    virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL) ;
    virtual void reset(void);
    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    citus_dut(string db,string ip, unsigned int port);
};

struct citus_worker_connection {
    shared_ptr<citus_dut> dut;
    string test_ip;
    string test_db;
    unsigned int test_port;
    bool connected_to_cluster;
    bool activated;
    bool has_database;
    citus_worker_connection(string ip, string db, unsigned int port);
};

struct citus_table : table{
    column* distribution_column;
    string citus_table_type;
    int colocation_id;
    int shard_count;
    string access_method;
    citus_table(table& t, column* distribution_column = NULL
    ,string citus_table_type = "distributed"
    ,int colocation_id = 0
    ,int shard_count = 32
    ,string access_method = "heap");
    citus_table(string name, string schema, bool insertable, bool base_table)
    :table(name, schema, insertable, base_table){}
        
    virtual int get_type(){
        if(citus_table_type == "distributed"){
            return 1;
        }else if(citus_table_type == "reference"){
            return 2;
        }
        return 0;
    }
};

struct schema_citus : schema_pqxx{
    map<string, shared_ptr<citus_table>> distributed_tables;
    map<string, shared_ptr<citus_table>> reference_tables;
    virtual void refresh();
    schema_citus(string db, string ip, unsigned int port, bool no_catalog);
};

struct citus_context : Context{
    std::vector<citus_worker_connection> workers;

    shared_ptr<schema_citus> get_schema(void){
        return dynamic_pointer_cast<schema_citus>(db_schema);
    }

    shared_ptr<citus_dut> get_dut(void){
        return dynamic_pointer_cast<citus_dut>(master_dut);
    }
};

// struct citus_action : action
// {
//     string sql;
//     virtual void random_fill(Context& ctx){};
//     virtual void out(std::ostream &out) override;
//     virtual bool run(Context& ctx) override;
//     virtual action* clone() const override {
//         return new citus_action(*this);
//     }
//     //citus_action(shared_ptr<dut_base> d);
// };



#endif