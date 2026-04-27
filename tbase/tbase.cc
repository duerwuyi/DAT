#include "tbase.hh"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <unistd.h>

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

tbase_dut::tbase_dut(string db,string ip, unsigned int port)
{
    postgres_dut = make_shared<dut_libpq>(db, ip, port);
}

void tbase_dut::reset_to_backup(void){
    //should be null, because tbase does not have any method to reset to backup without rebuilding the cluster
}

void tbase_dut::test(const string &stmt, 
                    vector<vector<string>>* output, 
                    int* affected_row_num,
                    vector<string>* env_setting_stmts){
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm now_tm = *std::localtime(&now_time_t);
    std::cout<<" tbase test: "  << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S")
              << '.' << std::setfill('0') << std::setw(5) << now_ms.count() << std::endl;
    try{
        postgres_dut -> test(stmt,output,affected_row_num,env_setting_stmts);
    }catch(exception& e){
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos);
        if(expected) throw;
        if(is_expected_error_in_distributed_query(err)){
            throw runtime_error("[tbase] expected error [" + err + "]");
        }
        else{
            throw runtime_error("[tbase] execution error [" + err + "]");
        }
    }

}

void tbase_dut::reset(void){
    cout<<"tbase reset"<<endl;
    while(true){
        try{
            string sql = "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();";
            postgres_dut -> test(sql);
            postgres_dut -> reset_without_force();
            break;
        }catch(exception& e){
            // do nothing
            cerr << "tbase_sql_dut::reset_to_backup" << e.what() << endl;
            sleep(6);
        }
    }
}

void tbase_dut::backup(void){
    //could not use pg_dump,so use single backup to reset.
}

void tbase_dut::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content){
    cout<<"tbase get_content"<<endl;
    return postgres_dut -> get_content(tables_name, content);
}

void schema_tbase::refresh(void){
    cout<<"tbase refresh_schema"<<endl;
    string sql = "select * from pgxc_node;";
    vector<vector<string>> output;
    dut -> test(sql, &output);
    auto row_num = output.size();
    for(int i = 0; i < row_num; i++){
        node_names.push_back(output[i][0]);
    }
    sql = "select * from pgxc_group;";
    dut -> test(sql, &output);
    row_num = output.size();
    for(int i = 0; i < row_num; i++){
        groups[output[i][0]].push_back(output[i][2]);
    }
}

schema_tbase::schema_tbase(string db, string ip, unsigned int port, bool no_catalog)
:schema_pqxx(db, ip, port, no_catalog){
    dut = make_shared<tbase_dut>(db, ip, port);
    //todo : add supported_setting
    //refresh();
}

// bool tbase_action::run(Context& ctx){
//     auto& context = dynamic_cast<tbase_context&>(ctx);
//     try{
//         context.master_dut -> test(sql);
//     }catch(exception& e){
//         string s = "#"+sql;
//         log(s, context.logfile);
//         throw;
//     }
//     log(sql, context.logfile);
//     return true;
// }

// void create_node_group::random_fill(Context& ctx){
//     auto& context = dynamic_cast<tbase_context&>(ctx);
//     //todo
// }

// void create_node_group::default_fill(Context& ctx){
//     auto& context = dynamic_cast<tbase_context&>(ctx);
//     context.tbase_schema -> refresh();
//     sql = "create default node group default_group  with (";
//     for(auto node_name : context.tbase_schema -> node_names){
//         sql += node_name + ",";
//     }
//     sql += ");";
// }

// void tbase_cluster_config_fuzzer::reset_init(){
//     context -> master_dut -> reset();
// }

// action_sequence tbase_cluster_config_fuzzer::mutate_and_run(action_sequence& victim){
//     return victim;
// }

// struct null_action : tbase_action{
//     virtual void random_fill(Context& ctx) override{
//         sql = "null_action";
//     }
// };

// tbase_cluster_config_fuzzer::tbase_cluster_config_fuzzer(string db, string ip, unsigned int port, shared_ptr<tbase_context> ctx)
// {
//     context = ctx;
//     base_context = ctx;
//     context -> master_dut = make_shared<tbase_dut>(db, ip, port);
//     name = "tbase_cluster_config_fuzzer";
//     action_sequence seed_template;
//     seed_template.push_back(make_shared<null_action>());
//     mutator = make_shared<action_sequence_mutator>(seed_template);
//     mutator -> factory -> register_action(make_shared<null_action>());
// }

// void tbase_cluster_fuzzer::ruin(){
// }

// tbase_cluster_fuzzer::tbase_cluster_fuzzer(string db, string ip, unsigned int port, shared_ptr<tbase_context> ctx)
// {
//     ctx->logfile = TBASE_SAVING_DIR + string(TBASE_CLUSTER_ACTIONS);
//     fuzzers.push_back(make_shared<tbase_cluster_config_fuzzer>(db,ip,port,ctx));
// }
