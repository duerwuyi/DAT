#ifndef CLICKHOUSE_HH
#define CLICKHOUSE_HH

#include "../schema.hh"
#include "../relmodel.hh"
#include "../dut.hh"
#include "../context/context.hh"
#include <iomanip>
#define CLICKHOUSE_SAVING_DIR "clickhouse/saved/"
#define CLICKHOUSE_DIST_DKEY_FILE "clickhouse_dkey.log"
using namespace std;

string get_cluster_name(string path = "clickhouse/cluster_config");

struct clickhouse_connection {
    string test_db;
    string test_ip;
    int test_port;
    string cluster;
    clickhouse_connection(string db,string ip, int port,bool distributed = true);
    ~clickhouse_connection() {};
};

struct clickhouse_table : table{
    string engine;
    string partition_key;
    column* partition_column;

    string dist_cluster;
    string sharding_key_expr;
    column* sharding_column;
    bool colocated;
    clickhouse_table(string name, string schema, bool insertable, bool base_table)
    :table(name,schema,insertable,base_table){}

    virtual int get_type(){
        if(colocated)
            return 3;
        if(engine == "Distributed"){
            return 2;
        }
        // if(engine == "MergeTree" && partition_key!=""){
        //     return 1;
        // }

        return 0;
    }
};

struct schema_clickhouse : schema, clickhouse_connection {
    schema_clickhouse(string db,string ip, int port,bool distributed = true);
    virtual string quote_name(const string &id) {
        return id;
    }
};

struct dut_clickhouse : dut_base, clickhouse_connection {
    string test_backup_file;
    
    virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
  
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);

    dut_clickhouse(string db,string ip, int port, string backup_file,bool distributed = true);
};

struct clickhouse_context : Context{
    shared_ptr<schema_clickhouse> get_schema(void){
        return dynamic_pointer_cast<schema_clickhouse>(db_schema);
    }

    shared_ptr<dut_clickhouse> get_dut(void){
        return dynamic_pointer_cast<dut_clickhouse>(master_dut);
    }
};

#endif