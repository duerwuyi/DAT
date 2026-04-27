#ifndef SHARDINGSPHERE_HH
#define SHARDINGSPHERE_HH
#include "../postgres.hh"
#include "../grammar.hh"
#ifdef HAVE_LIBMYSQLCLIENT
    #ifdef HAVE_MYSQL
    #include "../mysql.hh"
    #endif
#endif
#define SS_MYSQL_CONFIGURATION_FILE "shardingsphere/ss_mysql_config"
#define SS_PG_CONFIGURATION_FILE "shardingsphere/ss_pg_config"
#define SS_SAVING_DIR "shardingsphere/saved/"
#define SS_CLUSTER_ACTIONS "cluster_actions.sql"
#define SS_DIST_RECORD_FILE "ss_setup.sql"
#define SS_DIST_ALTER_FILE "ss_alter.sql"
#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

struct ss_dut : dut_base{
    shared_ptr<dut_base> dut;
    string db;
    string ip;
    unsigned int port;
    string single_dbms_name;

    virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL) ;
    virtual void reset(void);
    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    ss_dut(string db,string ip, unsigned int port,string single_dbms_name);
};

struct ss_worker_connection {
    shared_ptr<dut_base> dut;
    string test_ip;
    string test_db;
    unsigned int test_port;
    int id;
    string user;
    string password;
    bool connected_to_cluster;
    string storage_unit;
    ss_worker_connection(string ip, string db, unsigned int port,string single_dbms_name, int id, string user, string password);
};

struct ss_table : table{
    column* distribution_column;
    string ss_table_type;
    int colocation_id;
    int shard_count;
    string sharding_method;
    string single_table_storage;
    bool actually_exist = false;
    ss_table(table& t, column* distribution_column1 = NULL
    ,string ss_table_type1 = "distributed"
    ,int colocation_id1 = 0
    ,int shard_count1 = 4
    ,string sharding_method1 = "VOLUME_RANGE"):table(t.name, t.schema, t.is_insertable, t.is_base_table),
        distribution_column(distribution_column1),
        ss_table_type(ss_table_type1),
        colocation_id(colocation_id1),
        shard_count(shard_count1),
        sharding_method(sharding_method1){
            this -> cols = t.cols;
    }

    ss_table(string name, string schema, bool insertable, bool base_table)
    :table(name, schema, insertable, base_table){}
        
    virtual int get_type(){
        if(ss_table_type == "distributed"){
            return 1;
        }else if(ss_table_type == "reference" || ss_table_type == "broadcast"){
            return 2;
        }
        return 0;
    }
};

struct ss_schema : schema {
    string single_dbms_name;
    string db;
    string ip; 
    unsigned int port;
    shared_ptr<ss_dut> dut;
    map<OID, pg_type*> oid2type;
    map<string, pg_type*> name2type;

    ss_schema(string db,string ip, unsigned int port,string single_dbms_name);
    map<string, shared_ptr<ss_table>> distributed_tables;
    map<string, shared_ptr<ss_table>> broadcast_tables;
    map<string, shared_ptr<ss_table>> local_tables;
    map<string,vector<int>> table_stored_among_nodes;
    virtual void refresh();
    virtual bool fill_table_column(shared_ptr<ss_table> t);
    void postgres_setup(string db,string ip, unsigned int port);
    virtual std::string quote_name(const std::string &id){
        return id;
    }
#ifdef HAVE_MYSQL
    void mysql_setup(string db,string ip, unsigned int port);
#endif
};

struct ss_context : Context
{
    string single_dbms_name;
    vector<ss_worker_connection> workers;
    shared_ptr<create_table_stmt> temp_table;

    shared_ptr<dut_base> single_dut;

    shared_ptr<ss_schema> get_schema(void){
        return dynamic_pointer_cast<ss_schema>(db_schema);
    }

    shared_ptr<ss_dut> get_dut(void){
        return dynamic_pointer_cast<ss_dut>(master_dut);
    }
};

#endif