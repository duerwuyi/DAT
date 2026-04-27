#include "context_factory.hh"
#include "../citus/citus_sql_tester.hh"
#include "../shardingsphere/ss_sql_tester.hh"
#include "../tbase/tbase_sql_tester.hh"
#include "../clickhouse/clickhouse_cluster.hh"
#include "../vitess/vitess_cluster.hh"
#include "../vitess/vitess_action.hh"
#include "../ndb/ndb_cluster.hh"
#include "../ndb/ndb.hh"
#include "../tidb/tidb.hh"
#include "mysql.hh"

shared_ptr<citus_context> global_citus_context = make_shared<citus_context>();
shared_ptr<Context> global_citus_for_single_db = make_shared<Context>();
shared_ptr<ss_context> global_ss_context = make_shared<ss_context>();
shared_ptr<tidb_context> global_tidb_context = make_shared<tidb_context>();
// shared_ptr<tbase_context> global_tbase_context = make_shared<tbase_context>();
shared_ptr<clickhouse_context> global_clickhouse_context = make_shared<clickhouse_context>();
shared_ptr<vitess_context> global_vitess_context = make_shared<vitess_context>();
shared_ptr<ndb_context> global_ndb_context = make_shared<ndb_context>();

shared_ptr<Context> context_factory(dbms_info& d_info){
    if(!d_info.is_distributed){
        global_citus_for_single_db -> info = d_info;
        return global_citus_for_single_db;
    }
    if(d_info.distributed_db_name == "citus"){
        global_citus_context -> info = d_info;
        return global_citus_context;
    }
    // else if (d_info.distributed_db_name == "tbase") {
    //     global_tbase_context -> info = d_info;
    //     return global_tbase_context;
    // }
    else if (d_info.distributed_db_name == "shardingsphere") {
        global_ss_context -> single_dbms_name = d_info.dbms_name;
        global_ss_context -> info = d_info;
        return global_ss_context;
    }
    else if (d_info.distributed_db_name == "tidb") {
        global_tidb_context -> info = d_info;
        return global_tidb_context;
    }
    else if (d_info.distributed_db_name == "clickhouse") {
        global_clickhouse_context -> info = d_info;
        return global_clickhouse_context;
    }
    else if (d_info.distributed_db_name == "vitess") {
        global_vitess_context -> info = d_info;
        return global_vitess_context;
    }else if (d_info.distributed_db_name == "mysql_ndb") {
        global_ndb_context -> info = d_info;
        return global_ndb_context;
    }
    return nullptr;
}

void save_query(string dir, string filename, string& query)
{
    ofstream s(dir + "/" + filename);
    s << query << ";" << endl;
    s.close();
}

// distributed db setup
shared_ptr<dut_base> distributed_dut_setup(dbms_info& d_info){
    if(d_info.distributed_db_name == "citus"){
        return make_shared<citus_sql_dut>(d_info.test_db,d_info.test_ip, d_info.test_port);
    }
    // else if (d_info.distributed_db_name == "tbase") {
    //     return make_shared<tbase_sql_dut>(d_info.test_db,d_info.test_ip, d_info.test_port);
    // }
    else if (d_info.distributed_db_name == "shardingsphere") {
        return make_shared<ss_sql_dut>(d_info.test_db,d_info.test_ip, d_info.test_port, d_info.dbms_name);
    }
    else if(d_info.distributed_db_name == "tidb"){
        return make_shared<dut_tidb>(d_info.test_db,d_info.test_ip, d_info.test_port);
    }
    else if(d_info.distributed_db_name == "clickhouse"){
        return make_shared<dut_clickhouse>(d_info.test_db, d_info.test_ip, d_info.test_port, string(CLICKHOUSE_SAVING_DIR)+CLICKHOUSE_DIST_RECORD_FILE);
    }
    else if(d_info.distributed_db_name == "vitess"){
        return make_shared<dut_vitess>(d_info.test_db, d_info.test_ip, d_info.test_port, true);
    }
    else if(d_info.distributed_db_name == "mysql_ndb"){
        return make_shared<dut_mysql>(d_info.test_db, d_info.test_ip, d_info.test_port);
    }
    else {
        cerr << d_info.dbms_name << " is not installed, or it is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }
    return nullptr;
}

shared_ptr<dut_base> new_dut_setup(dbms_info& d_info)
{
    shared_ptr<dut_base> dut;
    if (d_info.is_distributed) {
        return distributed_dut_setup(d_info);
    }else if(d_info.dbms_name == "vitess"){
        return make_shared<dut_vitess>(d_info.test_db, d_info.test_ip, d_info.test_port, false);
    }
    else if (d_info.dbms_name == "mysql")
        dut = make_shared<dut_mysql>(d_info.test_db, d_info.test_ip, d_info.test_port);
    else if(d_info.dbms_name == "mysql_ndb"){
        return make_shared<dut_mysql>(d_info.test_db, d_info.test_ip, d_info.test_port);
    }
    else if (d_info.dbms_name == "clickhouse"){
        dut = make_shared<dut_clickhouse>(d_info.test_db, d_info.test_ip, d_info.test_port, string(DB_RECORD_FILE), false);
    }
    else if (d_info.dbms_name == "postgres")
        dut = make_shared<dut_libpq>(d_info.test_db,d_info.test_ip, d_info.test_port);
    else {
        cerr << d_info.dbms_name << " is not installed, or it is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }
    return dut;
}


shared_ptr<schema> distributed_get_schema(dbms_info& d_info){
    if(d_info.distributed_db_name == "citus"){
        return make_shared<schema_citus>(d_info.test_db, d_info.test_ip, d_info.test_port, true);
    }
    // else if (d_info.distributed_db_name == "tbase") {
    //     return make_shared<schema_tbase>(d_info.test_db, d_info.test_ip, d_info.test_port, true);
    // }
    else if (d_info.distributed_db_name == "shardingsphere") {
        return make_shared<ss_schema>(d_info.test_db, d_info.test_ip, d_info.test_port, d_info.dbms_name);
    }else if(d_info.distributed_db_name == "tidb"){
        return make_shared<schema_tidb>(d_info.test_db, d_info.test_ip, d_info.test_port);
    }
    else if(d_info.distributed_db_name == "clickhouse"){
        return make_shared<schema_clickhouse>(d_info.test_db, d_info.test_ip, d_info.test_port);
    }
    else if(d_info.distributed_db_name == "vitess"){
        return make_shared<schema_vitess>(d_info.test_db, d_info.test_ip, d_info.test_port, true);
    }else if(d_info.distributed_db_name == "mysql_ndb"){
        return make_shared<schema_mysql_ndb>(d_info.test_db, d_info.test_ip, d_info.test_port);
    }
    else {
        cerr << d_info.dbms_name << " is not installed, or it is not supported yet(distributed_get_schema)" << endl;
        throw runtime_error("Unsupported DBMS in distributed_get_schema");
    }
    return nullptr;
}

shared_ptr<schema> get_new_schema(dbms_info& d_info)
{
    shared_ptr<schema> schema;
    static int try_time = 0;

    //auto schema_start = get_cur_time_ms();
    try {
        if (d_info.is_distributed) {
            schema = distributed_get_schema(d_info);
        }
        else if(d_info.dbms_name == "vitess"){
            return make_shared<schema_vitess>(d_info.test_db, d_info.test_ip, d_info.test_port, false);
        }
        else if (d_info.dbms_name == "mysql") 
            schema = make_shared<schema_mysql>(d_info.test_db, d_info.test_ip, d_info.test_port);
        
        else if (d_info.dbms_name == "postgres")
            schema = make_shared<schema_pqxx>(d_info.test_db, d_info.test_ip, d_info.test_port, true);
        else if (d_info.dbms_name == "clickhouse")
            schema = make_shared<schema_clickhouse>(d_info.test_db, d_info.test_ip, d_info.test_port, false);
        else if(d_info.dbms_name == "mysql_ndb"){
            return make_shared<schema_mysql>(d_info.test_db, d_info.test_ip, d_info.test_port);
        }
        else {
            cerr << d_info.dbms_name << " is not supported yet" << endl;
            throw runtime_error("Unsupported DBMS");
        }
        //dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - schema_start);

    } catch (exception &e) { // may occur occastional error
        //dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - schema_start);
        
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos) || (err.find("timeout") != string::npos);
        if (!expected) {
            cerr << "unexpected error in get_schema: " << err << endl;
            cerr << "cannot save test case in get_schema"<< endl;
            abort(); // stop if trigger unexpected error
        }
        if (try_time >= 8) {
            cerr << "Fail in get_schema() " << try_time << " times, return" << endl;
	        throw;
        }
        try_time++;
        schema = get_new_schema(d_info);
        try_time--;
        return schema;
    }
    return schema;
}
