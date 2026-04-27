#include "common_fuzz_checker.hh"
#include <sstream>


int save_backup_file(string path, dbms_info& d_info)
{
    if (false) {}
    #ifdef HAVE_SQLITE
    else if (d_info.dbms_name == "sqlite")
        return dut_sqlite::save_backup_file(path, d_info.test_db);
    #endif

    #ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        return dut_mysql::save_backup_file(d_info.test_db, path);
    #endif
    #ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        return dut_mariadb::save_backup_file(path);
    #endif
    #ifdef HAVE_OCEANBASE
    else if (d_info.dbms_name == "oceanbase")
        return dut_oceanbase::save_backup_file(path);
    #endif
    #ifdef HAVE_MONETDB
    else if (d_info.dbms_name == "monetdb")
        return dut_monetdb::save_backup_file(path);
    #endif

    #ifdef HAVE_COCKROACH
    else if (d_info.dbms_name == "cockroach")
        return dut_cockroachdb::save_backup_file(path);
    #endif
    else if (d_info.dbms_name == "tidb") {
        return dut_tidb::save_backup_file(d_info.test_db + to_string(d_info.test_port), path);
    }
    else if (d_info.dbms_name == "postgres")
        return dut_libpq::save_backup_file(d_info.test_db + to_string(d_info.test_port), path);
    else if (d_info.dbms_name == "clickhouse") {
        string cmd = "cp " + string(DB_RECORD_FILE) + " " + path;
        return system(cmd.c_str());
    }
    else if (d_info.dbms_name == "vitess" ||d_info.dbms_name == "mysql_ndb") {
        return dut_mysql::save_backup_file(d_info.test_db, path);
    }
    else {
        cerr << d_info.dbms_name << " is not supported yet" << endl;
        // throw runtime_error("Unsupported DBMS");
    }
}

shared_ptr<cluster_fuzzer> get_cluster_fuzzer(shared_ptr<Context> context){
    if(context -> info.distributed_db_name == "citus" && dynamic_pointer_cast<citus_context>(context) != nullptr){
        auto ctx = dynamic_pointer_cast<citus_context>(context);
        return make_shared<citus_cluster_fuzzer>(ctx);
    }
    if(context -> info.distributed_db_name == "shardingsphere" && dynamic_pointer_cast<ss_context>(context) != nullptr){
        auto ctx = dynamic_pointer_cast<ss_context>(context);
        return make_shared<ss_cluster_fuzzer>(ctx);
    }
    if(context -> info.distributed_db_name == "tidb" && dynamic_pointer_cast<tidb_context>(context) != nullptr){
        auto ctx = dynamic_pointer_cast<tidb_context>(context);
        return make_shared<tidb_cluster_fuzzer>(ctx);
    }
    if(context -> info.distributed_db_name == "clickhouse" && dynamic_pointer_cast<clickhouse_context>(context) != nullptr){
        auto ctx = dynamic_pointer_cast<clickhouse_context>(context);
        return make_shared<clickhouse_cluster_fuzzer>(ctx);
    }
    if(context -> info.distributed_db_name == "vitess" && dynamic_pointer_cast<vitess_context>(context) != nullptr){
        auto ctx = dynamic_pointer_cast<vitess_context>(context);
        return make_shared<vitess_cluster_fuzzer>(ctx);
    }if(context -> info.distributed_db_name == "mysql_ndb" && dynamic_pointer_cast<ndb_context>(context) != nullptr){
        auto ctx = dynamic_pointer_cast<ndb_context>(context);
        return make_shared<ndb_cluster_fuzzer>(ctx);
    }
    return nullptr;
}

void common_fuzz_checker::log(string s, string path){
    // std::ofstream log_file;
    // log_file.open(path, std::ios_base::app); // 
    // log_file << s << ";" <<endl<<endl;
    // log_file.close();
}

common_fuzz_checker::common_fuzz_checker(shared_ptr<Context> context, shared_ptr<Context> compared, bool need_schema, bool should_change_schema, int total_query_count){
    checker_start_time = std::chrono::steady_clock::now();
    this->total_query_count = total_query_count;
    this->tester = make_shared<ddc_tester>(context);
    this->compared = compared;

    tester -> context -> master_dut = new_dut_setup(tester -> context -> info);
    // tester -> context -> db_schema = get_new_schema(tester -> context -> info);
    this->generator = make_shared<diff_db_mutator>(context, compared);
    this->fuzzer = get_cluster_fuzzer(context);

    // // if master_dut is not set, set it
    // if(tester->context -> master_dut == nullptr){
    //     tester -> context -> master_dut = new_dut_setup(tester -> context -> info);
    // }

    // // if should change schema, use diff_db_mutator to generate new db, then get new schema
    // if(should_change_schema) {
    //     this->generator = make_shared<diff_db_mutator>(context, compared);
    //     this->cluster_fuzzer = get_cluster_fuzzer(context);
    //     return;
    // }
    // // if need schema, get new schema
    // if(need_schema && tester->context -> db_schema == nullptr){
    //     tester -> context -> db_schema = get_new_schema(tester -> context -> info);
    // }
}


long long common_fuzz_checker::elapsed_runtime_seconds() const {
    auto elapsed = std::chrono::steady_clock::now() - checker_start_time;
    return std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
}

string common_fuzz_checker::elapsed_runtime_hms() const {
    long long seconds = elapsed_runtime_seconds();
    long long hours = seconds / 3600;
    seconds %= 3600;
    long long minutes = seconds / 60;
    seconds %= 60;

    std::ostringstream out;
    out << hours << "h " << minutes << "m " << seconds << "s";
    return out.str();
}

void common_fuzz_checker::run(){
    fuzzer -> reset_init();
    //tester->context->db_schema = generator->init_db();
    tester->context->db_schema = generator->init_db_without_compare();
    
    // generate_query_pool();
    JsonClient client("127.0.0.1", 9000);
    client.connect();

    //send action list to server
    client.send_action_list(get_dds_list(tester->context->db_schema));
    
    //run differential testing
    for(int i = 0; i < total_query_count; i++){
        shared_ptr<query> q = nullptr;
        if(d6()<3){
            q = random_pick(valid_query_pool);
        }
        else{
            q = random_pick(invalid_query_pool);
        }
        tester->test_with_query_plan(*q);
        // for(int j = 0; j < 5; j++){
        //     //extract feature
        //     extracter e;
        //     e.visit(q->ast.get());
        //     query_feature feature = e.extract_feature(tester->context->db_schema);
        //     //send usage to server by json & get action from server
        //     auto response = client.send_usage(feature);
        //     //exectue action
        //     action a = get_action_from_server(tester->context->db_schema, response);
        //     a.qm->visit(q->ast.get());
        // }
    }
}
void common_fuzz_checker::init_db(){
    generator->init_db();
}

string common_fuzz_checker::generate_vaild_query(){
    while(true){
        auto query = tester->generate_query();
        if(tester->test(*query)){
            return query -> query_str;
        }
    }
}

void common_fuzz_checker::generate_query_pool(){
    for(int i = 0; i < total_query_count; i++){
        auto q = tester->generate_query();
        tester->test_with_query_plan(*q);
        // if(run_and_check(q, false)){
        //     valid_query_pool.push_back(q);
        // }
        // else{
        //     invalid_query_pool.push_back(q);
        // }
    }
    cout << "total_query_count: " << total_query_count << endl;
    cout << "valid_query_pool size: " << valid_query_pool.size() << endl;
    cout << "invalid_query_pool size: " << invalid_query_pool.size() << endl;
}

bool common_fuzz_checker::check(multiset<row_output>& r1, multiset<row_output>& r2){
    cerr << "compare select result: ";
    cerr << "single: " << r1.size() << ", ";
    cerr << "distributed: " << r2.size() << endl;

    if (r1 != r2) {
        //print r1
        cout<<"single result:"<<endl;
        for(auto& r: r1){
            for(auto item : r){
                cout<<item<<" ";
            }
            cout<<endl;
        }

        cout<<"distributed result:"<<endl;
        for(auto& r: r2){
            for(auto item : r){
                cout<<item<<" ";
            }
            cout<<endl;
        }
        cerr << "maybe a logic bug !!!" << endl;
        return false;
    }
    //if valid query & result is the same, return true
    return true;
}

bool common_fuzz_checker::run_and_check(shared_ptr<query> q, bool ensure_valid){
    shared_ptr<query> compared_query = make_shared<query>(q -> ast);
    //new tester
    auto compared_tester = make_shared<ddc_tester>(compared);
    compared_tester->test(*compared_query);

    tester->test(*q);
    if(!ensure_valid && !q->is_valid){
        //if invalid query, return false
        cerr << "invalid query."<< endl;
        return false;
    }

    if (!check(q->result,compared_query->result)) {
        cerr << "maybe a logic bug !!!" << endl;
        //save query
        save_query(".", "unexpected.sql", q->query_str);
        save_backup_file(".", tester->context->info);
        //if logic bug, abort
        abort();
    }
    //if valid query & result is the same, return true
    return true;
}

void common_fuzz_checker::print_result(){
    cout << "total_query_count: " << total_query_count << endl;
    cout << "success_query_count: " << tester->success_query_count << endl;
    cout << "failed_query_count: " << tester->failed_query_count << endl;
    cout << "result_not_null_query_count: " << tester->result_not_null_query_count << endl;

    float valid_query_rate = (float)tester->success_query_count / total_query_count;
    float not_null_query_rate = (float)tester->result_not_null_query_count / tester->success_query_count;
    cout << "valid_query_rate: " << valid_query_rate * 100 << "%" << endl;
    cout << "not_null_query_rate: " << not_null_query_rate * 100 << "%" << endl;
}