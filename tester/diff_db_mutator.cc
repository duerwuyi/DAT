#include "diff_db_mutator.hh"
#include <citus/citus_cluster_fuzzer.hh>
#include <citus/citus.hh>
#include <citus/citus_sql_tester.hh>
// #include <tbase/tbase_sql_tester.hh>
#include <shardingsphere/ss_cluster_fuzzer.hh>
#include <shardingsphere/ss_sql_tester.hh>
#include <tidb/tidb_cluster.hh>
#include <clickhouse/clickhouse_cluster.hh>
#include <vitess/vitess_cluster.hh>
#include <vitess/vitess_action.hh>
#include <ndb/ndb_cluster.hh>
#include "../globals.h"

shared_ptr<distributor> distributor_factory(dbms_info& d_info, shared_ptr<Context> ctx){
    if(d_info.distributed_db_name == "citus"){
        auto context = dynamic_pointer_cast<citus_context>(ctx);
        return make_shared<citus_distributor>(context);
    }
    // }else if (d_info.distributed_db_name == "tbase") {
    //     auto context = dynamic_pointer_cast<tbase_context>(ctx);
    //     return make_shared<tbase_distributor>(context);
    // }
    else if (d_info.distributed_db_name == "shardingsphere") {
        auto context = dynamic_pointer_cast<ss_context>(ctx);
        return make_shared<ss_distributor>(context);
    }else if (d_info.distributed_db_name == "tidb") {
        auto context = dynamic_pointer_cast<tidb_context>(ctx);
        return make_shared<tidb_distributor>(context);
    }
    else if (d_info.distributed_db_name == "clickhouse") {
        auto context = dynamic_pointer_cast<clickhouse_context>(ctx);
        return make_shared<clickhouse_distributor>(context);
    }
    else if (d_info.distributed_db_name == "vitess") {
        auto context = dynamic_pointer_cast<vitess_context>(ctx);
        return make_shared<vitess_distributor>(context);
    }else if (d_info.distributed_db_name == "mysql_ndb") {
        auto context = dynamic_pointer_cast<ndb_context>(ctx);
        return make_shared<ndb_distributor>(context);
    }
    else {
        cerr << d_info.distributed_db_name << " has no distributor" << endl;
        throw runtime_error("Unsupported DBMS");
    }
}

// shared_ptr<rebalancer> rebalancer_factory(dbms_info& d_info, shared_ptr<Context> ctx){
//     if(d_info.distributed_db_name == "citus"){
//         auto context = dynamic_pointer_cast<citus_context>(ctx);
//         // use a new dut to avoid the problem of connection reuse
//         context -> master_dut = make_shared<citus_dut>(d_info.test_db, d_info.test_ip, d_info.test_port);
//         return make_shared<citus_rebalancer>(context);
//     }else if (d_info.distributed_db_name == "tbase") {
//         auto context = dynamic_pointer_cast<tbase_context>(ctx);
//         return make_shared<tbase_rebalancer>(context);
//     }
//     else if (d_info.distributed_db_name == "shardingsphere") {
//         auto context = dynamic_pointer_cast<ss_context>(ctx);
//         return make_shared<ss_rebalancer>(context);
//     }
//     else {
//         cerr << d_info.dbms_name << " has no rebalancer" << endl;
//         throw runtime_error("Unsupported DBMS");
//     }
// }

diff_db_mutator::diff_db_mutator(shared_ptr<Context> distri_ctx, shared_ptr<Context> single_ctx)
    : distri_context(distri_ctx), single_context(single_ctx) {
    distri_db_info = distri_context -> info;
    single_db_info = single_context -> info;
}

shared_ptr<prod> interect_test(shared_ptr<Context> reference_ctx, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    bool need_affect,
                    string record_file,
                    int i = 0,
                    shared_ptr<prod> generator_prod = NULL,
                    shared_ptr<distributor> distributor = NULL)
{
    auto d_info = reference_ctx -> info;
    auto schema = get_new_schema(d_info);
    scope scope;
    schema->fill_scope(scope);
    shared_ptr<prod> gen;
    if(generator_prod == NULL)  
        gen = tmp_statement_factory(&(scope));
    else if(dynamic_pointer_cast<create_table_stmt>(generator_prod) != NULL){
        shared_ptr<create_table_stmt> create_table = dynamic_pointer_cast<create_table_stmt>(generator_prod);
        gen = make_shared<create_table_stmt>(nullptr, &(scope), create_table);
        // create_table->created_table->name = unique_table_name(&(scope));
        // gen = create_table;
    }else{
        gen = generator_prod;
    }
    ostringstream s;
    gen->out(s);
    auto sql = s.str() + ";";

    static int try_time = 0;
    //auto test_start = get_cur_time_ms();
    try {
        shared_ptr<dut_base> dut = reference_ctx->master_dut != NULL ? reference_ctx->master_dut : new_dut_setup(d_info);
        int affect_num = 0;
        if(d_info.is_distributed && distributor != NULL) {
            //test in distributed mode
            sql = distributor -> distribute_one(gen, &(affect_num), i);
        }else{
            dut->test(sql, NULL, &(affect_num));
        }
        if (need_affect && affect_num <= 0)
            throw runtime_error(string("expected error: affect result empty"));
        
        ofstream ofile(record_file, ios::app);
        ofile << sql << endl;
        ofile.close();
        //dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);

    } catch(std::exception &e) { // ignore runtime error
        //dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);
        
        string err = e.what();
        cerr << "err: " << e.what() << endl;
        bool expected = (err.find("expected error") != string::npos) || (err.find("timeout") != string::npos);
        if (!expected) {
            save_query(".", "unexpected.sql", sql);
            cerr << "unexpected error in interect_test: " << err << endl;
            cerr << "cannot save backup as generating db is not finished"<< endl;
            abort(); // stop if trigger unexpected error
        }
        if (try_time >= 8) {
            cerr << "Fail in interect_test() " << try_time << " times, return" << endl;
            cerr << "err: " << e.what() << endl;
            throw;
        }
        try_time++;
        interect_test(reference_ctx, tmp_statement_factory, need_affect, record_file);
        try_time--;
    }
    return gen;
}

void init_schema(shared_ptr<Context> reference, shared_ptr<Context> passive) {
    total_groups = d6() + 3; // at least 3 statements to create 3 tables
    auto d_info = reference -> info;
    // int tables_in_a_group = dx(3) + 5;
    int tables_in_a_group = 5;
    auto distributor = distributor_factory(d_info, reference);
    distributor -> clear_record_file();
    vector<shared_ptr<prod>> gens;
    for (auto i = 0; i < total_groups; i++){
        auto gen = interect_test(reference, &ddl_statement_factory, false, DB_RECORD_FILE, i, NULL, distributor); // has disabled the not null, check and unique clause 
        gens.push_back(gen);
        ostringstream s;
        gen->out(s);
        auto sql = s.str() + ";";
        if(passive != nullptr && reference->info.distributed_db_name != "vitess") passive -> master_dut -> test(sql, NULL, NULL);
    }
    for (auto i = total_groups; i < total_groups * tables_in_a_group; i++){
        auto gen = interect_test(reference, &ddl_statement_factory, false, DB_RECORD_FILE, i, gens[i % total_groups], distributor);
        ostringstream s;
        gen->out(s);
        auto sql = s.str() + ";";
        if(passive != nullptr && reference->info.distributed_db_name != "vitess") passive -> master_dut -> test(sql, NULL, NULL);
    }
    
}

string normal_test(shared_ptr<Context> reference_ctx, 
                    shared_ptr<schema> schema, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *, table *), 
                    bool need_affect,
                    string record_file,
                    table *target_table = NULL)
{
    auto d_info = reference_ctx -> info;
    scope scope;
    schema->fill_scope(scope);
    shared_ptr<prod> gen = tmp_statement_factory(&(scope), target_table);
    ostringstream s;
    gen->out(s);
    auto sql = s.str() + ";";

    static int try_time = 0;
    // auto test_start = get_cur_time_ms();
    try {
        shared_ptr<dut_base> dut = reference_ctx->master_dut != NULL ? reference_ctx->master_dut : new_dut_setup(d_info);
        int affect_num = 0;
        dut->test(sql, NULL, &(affect_num));
        if (need_affect && affect_num <= 0 && dynamic_pointer_cast<insert_stmt>(gen)) {
            throw runtime_error(string("expected error: affect result empty"));
        }

        ofstream ofile(record_file, ios::app);
        ofile << sql << endl;
        ofile.close();
        if(dynamic_pointer_cast<clickhouse_context>(reference_ctx)){
            ofstream ofile(string(CLICKHOUSE_SAVING_DIR)+CLICKHOUSE_DIST_RECORD_FILE, ios::app);
            ofile << sql << endl;
            ofile.close();
        }
        //dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);

    } catch(std::exception &e) { // ignore runtime error
        //dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);

        string err = e.what();
        bool expected = (err.find("expected error") != string::npos) || (err.find("timeout") != string::npos);
        if (!expected) {
            save_query(".", "unexpected.sql", sql);
            cerr << "unexpected error in normal_test: " << err << endl;
            cerr << "cannot save backup as generating db is not finished"<< endl;
            abort(); // stop if trigger unexpected error
        }
        if (try_time >= 500) {
            cerr << "Fail in normal_test() " << try_time << " times, return" << endl;
            cerr << err << endl;
            throw;
        }
        try_time++;
        sql = normal_test(reference_ctx, schema, tmp_statement_factory, need_affect, record_file, target_table);
        try_time--;
    }
    return sql;
}

void diff_db_mutator::elimitate_inconsistent_data(shared_ptr<cluster_fuzzer> fuzzer) {
    // elimitate the inconsistent data between distri db and single db
    if(dynamic_pointer_cast<citus_context>(distri_context)){
        assert(fuzzer != nullptr);
        fuzzer -> reset_init();
        distri_context -> master_dut -> reset_to_backup();
        single_context -> master_dut -> reset_to_backup();
        cout<<"elimitate_inconsistent_data"<<endl;
        return;
    }
    if(dynamic_pointer_cast<tidb_context>(distri_context) || dynamic_pointer_cast<ndb_context>(distri_context)){
        // auto schema = distri_context -> db_schema;
        // for (auto &t : schema -> tables) {
        //     string table_name = t.ident();
        //     string sql = "check table " + table_name + ";";
        // }
        distri_context -> master_dut -> reset_to_backup();
        single_context -> master_dut -> reset_to_backup();
        cout<<"elimitate_inconsistent_data"<<endl;
        return;
    }

    auto schema = distri_context -> db_schema;
    for (auto &t : schema -> tables) {
        string table_name = t.ident();
        string sql = "select * from " + table_name + ";";
        stmt_output distri_output;
        stmt_output single_output;
        distri_context -> master_dut -> test(sql, &distri_output, NULL);
        single_context -> master_dut -> test(sql, &single_output, NULL);
        multiset<row_output> distri_set;
        multiset<row_output> single_set;
        for (auto &row : distri_output) {
            distri_set.insert(row);
        }
        for (auto &row : single_output) {
            single_set.insert(row);
        }
        if(distri_set == single_set && distri_set.size() > 0) {
            continue;
        }
        // elimitate the inconsistent data
        cout << "elimitate inconsistent data in table: " << table_name << endl;
        // delete all data in distri db
        string delete_sql = "delete from " + table_name + ";";
        distri_context -> master_dut -> test(delete_sql, NULL, NULL);
        // insert data from single db to distri db
        auto basic_dml_stmt_num = dx(40)+30;
        string sql1;
        for (auto i = 0; i < basic_dml_stmt_num; i++) {
            sql1 = normal_test(distri_context, schema, &basic_dml_statement_factory, true, DB_RECORD_FILE, &t);
            if(single_context != nullptr){
                single_context -> master_dut -> test(sql1, NULL, NULL);
            } 
        }
    }
}

void init_dml(shared_ptr<Context> reference, shared_ptr<Context> passive) {
    //auto d_info = reference -> info;
    auto schema = reference -> db_schema;
    for (auto &t : schema -> tables) {
        auto basic_dml_stmt_num = dx(40)+30;
        string sql;
        for (auto i = 0; i < basic_dml_stmt_num; i++) {
            sql = normal_test(reference, schema, &basic_dml_statement_factory, true, DB_RECORD_FILE, &t);
            if(passive != nullptr) passive -> master_dut -> test(sql, NULL, NULL);
        }
    }
}

shared_ptr<schema> diff_db_mutator::init_db() {
    if (remove(DB_RECORD_FILE) != 0) {
        cerr << "generate_database: cannot remove file (" << DB_RECORD_FILE << ")" << endl;
    }
    while(true){
        try
        {
            clear_naming_data();
            // distri_context -> master_dut -> reset();
            // single_context -> master_dut -> reset();

            //the function in later generated query is from single db
            if(single_context != nullptr) 
                single_context -> db_schema = get_new_schema(single_context -> info);
            
            init_schema(distri_context, single_context);

            distri_context -> db_schema = get_new_schema(distri_db_info);
            //init the dml
            init_dml(distri_context, single_context);

            //backup the schema
            distri_context -> master_dut -> backup();
            single_context -> master_dut -> backup();
            single_context -> db_schema = get_new_schema(single_db_info);
            single_context -> master_dut = new_dut_setup(single_db_info);

            if(auto tidb_ctx = dynamic_pointer_cast<tidb_context>(distri_context)){
                wait_tiflash_replicas_ready(tidb_ctx, 100 ,1000);
            }

            break;
        }catch(std::exception &e){
            sleep(60);
            cerr << "exception in init_db: " << e.what() << ", retrying..."<< endl;
            continue;
        }
    }
    
    return get_new_schema(distri_db_info);
}

shared_ptr<schema> diff_db_mutator::init_db_without_compare() {
    if (remove(DB_RECORD_FILE) != 0) {
        cerr << "generate_database: cannot remove file (" << DB_RECORD_FILE << ")" << endl;
    }
    clear_naming_data();
    
    init_schema(distri_context, nullptr);

    distri_context -> db_schema = get_new_schema(distri_db_info);
    distri_context -> master_dut = new_dut_setup(distri_db_info);
    //init the dml
    // init_dml(distri_context, nullptr);

    //backup the schema
    distri_context -> master_dut -> backup();
    return get_new_schema(distri_db_info);
}
