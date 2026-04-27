#include "citus.hh"
#include <algorithm>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <iomanip>
#include "../feedback/action.hh"
static bool is_expected_error_in_distributed_query(string error){
    if (error.find("only support for hash distributed tables") != string::npos
        || error.find("does not support") != string::npos
        || error.find("does not exist") != string::npos
        || error.find("value is too big in tsquery") != string::npos
        || (error.find("index row requires") != string::npos && error.find("bytes, maximum size") != string::npos)
        || error.find("invalid memory alloc request size") != string::npos
        || error.find(" must be ahead of ") != string::npos
        || error.find("unterminated format() type specifier") != string::npos
        || error.find("operand, lower bound, and upper bound cannot be NaN") != string::npos
        || error.find("Unicode normalization can only be performed if server encoding is UTF8") != string::npos
        || error.find("Cannot enlarge string buffer containing") != string::npos
        || error.find("nvalid input syntax for type numeric: ") != string::npos
        || error.find("numeric field overflow") != string::npos
        || error.find("could not create unique index") != string::npos
        || error.find("Unicode categorization can only be performed if server encoding is UTF8") != string::npos
        || error.find("invalid type name") != string::npos
        || error.find("complex joins are only supported when all distributed tables are co-located and joined on their distribution columns") != string::npos
        || error.find("currently unsupported") != string::npos
        || error.find("not support") != string::npos
        || error.find("cannot create constraint") != string::npos
        || error.find("not allowed") != string::npos
        //|| error.find("canceling statement due to statement timeout") != string::npos
        || error.find("not distributed") != string::npos
        || error.find("must not be VOLATILE") != string::npos
        || error.find("cannot push down") != string::npos
        || error.find("cannot perform") != string::npos
        || error.find("Subqueries in HAVING cannot refer to outer query") != string::npos
        || error.find("STABLE functions used in UPDATE queries cannot be called with column references") != string::npos
        || error.find("cannot perform an INSERT with NULL in the partition column") != string::npos
        || error.find("requires repartitioning") != string::npos //should remove
        || error.find("cannot be NULL") != string::npos
        || error.find("no worker with all shard placements") != string::npos
        || error.find("complex joins") != string::npos
        || error.find("only reference tables may be queried when targeting a reference table with multi shard UPDATE/DELETE queries with multiple table") != string::npos 
        )
    return true;
}

citus_table::citus_table(table& t, column* distribution_column1,string citus_table_type1,int colocation_id1,int shard_count1,string access_method1)
:table(t.name, t.schema, t.is_insertable, t.is_base_table),
distribution_column(distribution_column1),
citus_table_type(citus_table_type1),
colocation_id(colocation_id1),
shard_count(shard_count1),
access_method(access_method1){
    this -> cols = t.cols;
}

void schema_citus::refresh(void){
    cerr << "Loading citus tables..." << endl;
    string load_table_sql = "SELECT * FROM citus_tables;";
    if(conn){
        PQfinish(conn);
    }
    conn = PQsetdbLogin(test_ip.c_str(), to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), "postgres", "123abc");
    PGresult * res;
    try{
        res = pqexec_handle_error(conn, load_table_sql);
    }catch(exception& e){
        cout<<"Connection to PostgreSQL failed"<<PQerrorMessage(conn)<<endl;
        return;
    }
    auto row_num = PQntuples(res);
    for (int i = 0; i < row_num; i++) {
        dds d;
        string table_name(PQgetvalue(res, i, 0));
        auto target_table = *std::find_if(tables.begin(),tables.end(),
                [table_name](const table& table) {
                    return table.name == table_name;
                });
        auto citustable = make_shared<citus_table>(target_table);
        d.params["victim"] = string(PQgetvalue(res, i, 0));
        citustable -> citus_table_type = string(PQgetvalue(res, i, 1));
        d.params["type"] = string(PQgetvalue(res, i, 1)) == "reference" ? "1" : "2";
        citustable -> colocation_id = atoi(PQgetvalue(res, i, 3));
        d.params["colocate"] = string(PQgetvalue(res, i, 3));
        citustable -> shard_count = atoi(PQgetvalue(res, i, 5));
        d.params["shard_count"] = string(PQgetvalue(res, i, 5));
        string dkey = string(PQgetvalue(res, i, 2));
        for(column& c_col : citustable -> columns()){
            if(c_col.name == dkey){
                citustable -> distribution_column = &c_col;
                break;
            }
        }
        d.params["dkey"] = dkey;
        citustable -> access_method = string(PQgetvalue(res, i, 7));
        // d.params["access_method"] = string(PQgetvalue(res, i, 7));
        if(citustable -> citus_table_type == "reference"){
            reference_tables[target_table.name] = citustable;
        }else if(citustable -> citus_table_type == "distributed"){
            distributed_tables[target_table.name] = citustable;
        }
        this -> register_dds(citustable, d);
    }
    //find local tables
    for(auto& t : tables){
        if(distributed_tables.count(t.name) == 0 && reference_tables.count(t.name) == 0){
            dds d;
            d.params["victim"] = t.name;
            d.params["type"] = "0";
            d.params["colocate"] = "-1";
            d.params["shard_count"] = "1";
            d.params["dkey"] = "<none>";
            this -> register_dds(make_shared<table>(t), d);
        }
    }
    PQclear(res);
}

schema_citus::schema_citus(string db,string ip, unsigned int port, bool no_catalog)
:schema_pqxx(db, ip, port, no_catalog)
{
    target_ddbms = "citus";
    refresh();
    supported_setting["citus.local_table_join_policy"] = vector<string>({"auto", "'prefer-local'", "'prefer-distributed'"});
    supported_setting["citus.rebalancer_by_disk_size_base_cost"] = vector<string>({"0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512"});
    supported_setting["citus.max_cached_conns_per_worker"] = vector<string>({"0", "1", "2", "4", "8", "16", "32", "64", "128"});
    //supported_setting["citus.stat_statements_purge_interval"] = vector<string>({"0", "1", "2","3", "4","5", "8", "16", "32"});
    supported_setting["citus.stat_statements_track"] = vector<string>({"default", "none"});
    supported_setting["citus.local_table_join_policy "] = vector<string>({"on", "off"});
    supported_setting["citus.task_assignment_policy"] = vector<string>({"greedy", "'round-robin'","'first-replica'"});
    supported_setting["citus.enable_non_colocated_router_query_pushdown"] = vector<string>({"true", "false"});
    supported_setting["citus.enable_binary_protocol"] = vector<string>({"on", "off"});
    supported_setting["citus.enable_repartition_joins"] = vector<string>({"on", "off"});
    supported_setting["citus.enable_schema_based_sharding"] = vector<string>({"on", "off"});
    supported_setting["citus.citus.multi_shard_modify_mode"] = vector<string>({"parallel", "sequential"});
    supported_setting["citus.enable_statistics_collection"] = vector<string>({"on", "off"});
    supported_setting["citus.propagate_set_commands"] = vector<string>({"none", "local"});
}

void citus_dut::reset_to_backup(void){
    //should be null, because citus does not have any method to reset to backup without rebuilding the cluster
}

void citus_dut::test(const string &stmt, 
                    vector<vector<string>>* output, 
                    int* affected_row_num,
                    vector<string>* env_setting_stmts){
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm now_tm = *std::localtime(&now_time_t);
    std::cout<<" citus test: "  << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S")
              << '.' << std::setfill('0') << std::setw(5) << now_ms.count() << std::endl;
    try{
        postgres_dut -> test(stmt,output,affected_row_num,env_setting_stmts);
    }catch(exception& e){
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos);
        if(expected) throw;
        if(is_expected_error_in_distributed_query(err)){
            throw runtime_error("[citus] expected error [" + err + "]");
        }
        else{
            throw runtime_error("[citus] execution error [" + err + "]");
        }
    }

}

void citus_dut::reset(void){
    cout<<"citus reset"<<endl;
    postgres_dut -> reset();
    string create_sql = "CREATE EXTENSION IF NOT EXISTS citus;";
    postgres_dut->test(create_sql);
}

void citus_dut::backup(void){
    cout<<"citus backup"<<endl;
    return postgres_dut -> backup();
}

void citus_dut::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content){
    cout<<"citus get_content"<<endl;
    return postgres_dut -> get_content(tables_name, content);
}

citus_dut::citus_dut(string db, string ip, unsigned int port)
{
    postgres_dut = make_shared<dut_libpq>(db, ip, port);
}

citus_worker_connection::citus_worker_connection(string ip, string db, unsigned int port)
: test_db(db),test_ip(ip),test_port(port)
{
    dut = make_shared<citus_dut>(db, ip, port);
    connected_to_cluster = false;
    has_database = false;
}
