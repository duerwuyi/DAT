#include "citus_sql_tester.hh"
#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

void citus_sql_dut::reset(void){
    cout<<"citus_sql_dut reset"<<endl;
    shared_ptr<citus_context> context = make_shared<citus_context>();
    citus_dut::reset();
    // citus_cluster_fuzzer fuzzer(postgres_dut -> test_db, postgres_dut -> test_ip ,postgres_dut -> test_port,context);
    // fuzzer.reset_init();
}

void citus_sql_dut::reset_to_backup(void){
    cout<<"citus_sql_dut reset_to_backup"<<endl;
    

    postgres_dut -> load_backup_schema();
    vector<string> lines;
    string line;
    std::ifstream infile(CITUS_SAVING_DIR + string(CITUS_DIST_RECORD_FILE));
    if (!infile.is_open()) {
        std::cout << "Error opening file: " << CITUS_SAVING_DIR + string(CITUS_DIST_RECORD_FILE) << std::endl;
    throw runtime_error("Error opening file");
    }
    while (std::getline(infile, line)) {
        if (!line.empty() && line[0] != '#') {
            cout << line <<endl;
            postgres_dut -> test(line);
            // regex pattern(R"((.*?);[\s]*([^\s]+)[\s]+(\d+)[\s]+([^\s]+))");
            // smatch matches;
            
            // if (regex_match(line, matches, pattern)) {
            //     sql = matches[1].str() + ";";  
            //     ip = matches[2].str();         
            //     port = stoi(matches[3].str());
            //     db = matches[4].str();        
                
            //     dut = make_shared<citus_dut>(db, ip, port);
            //     dut->test(sql);
            //     log_query(sql + " " + ip + " " + to_string(port) + " " + db);
            // } else {
            //     std::cerr << "Invalid line format: " << line << std::endl;
            // }
        }
    }
    infile.close();

    postgres_dut -> load_backup_data();
}

static inline void trim_inplace(std::string& s) {
    auto not_space = [](int ch){ return !std::isspace(ch); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
    s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
}

vector<string> citus_sql_dut::get_query_plan(const std::vector<std::vector<std::string>>& query_plan) {
    //  Distributed Subplan 
    const regex re_ds_truncate(R"((^\s*->\s*Distributed Subplan)\b.*$)");
    const regex re_node(R"(^\s*Node:.*$)");

    // 
    const regex re_paren(R"(\([^()]*\))");

    // 
    const regex re_spaces(R"(\s{2,})");

    //  "Key:" 
    const regex re_key_truncate(R"((^.*?\bKey:).*$)");

    //  "on"  "on"
    // \bon\b  WindowAgg 
    const regex re_on_truncate(R"((^.*?\bon)\b.*$)");

    vector<string> result;
    for (const auto& row : query_plan) {
        if (row.empty()) continue;
        std::string line = row[0];

        if (regex_match(line, re_node)) continue;
        // 1)  "-> Distributed Subplan ..."  "-> Distributed Subplan"
        //     6_1
        line = regex_replace(line, re_ds_truncate, "$1");

        // 2)  cost/rows/width 
        while (regex_search(line, re_paren)) {
            line = regex_replace(line, re_paren, "");
        }

        line = regex_replace(line, re_key_truncate, "$1");

        line = regex_replace(line, re_on_truncate, "$1");

        // 5) 
        line = regex_replace(line, re_spaces, " ");
        trim_inplace(line);

        if (line.empty()) continue;
        result.push_back(line);
    }

    return result;
}

// void citus_distributor::distribute_all(void){
//     cout<<"citus_distributor distribute_all"<<endl;
//     // initialize the db_schema
//     if(context -> db_schema == nullptr){
//         context -> db_schema = make_shared<schema_citus>(
//             context -> get_dut() -> postgres_dut -> test_db, 
//             context -> get_dut() -> postgres_dut -> test_ip, 
//             context -> get_dut() -> postgres_dut -> test_port, 
//             true);
//     }
//     // Generate random sequence length 
//     int sequence_length = 5 + dx(12);
    
//     // Generate and execute action sequence
//     for (int i = 0; i < sequence_length; i++) {
//         try {
//             // Randomly pick an action type
//             auto action = manager->factory->random_pick();
//             // First try to fill the action with random parameters
//             try{
//                 action->random_fill(*context);
//             }catch(const runtime_error& e){
//                 string err = e.what();
//                 if(err.find("No candidates available") != string::npos){
//                     cout << "No more tables to distribute, in distributing." << endl;
//                     continue;
//                 }
//                 throw;
//             }
//             // Then try to run it
//             action->run(*context);
//         } catch (const runtime_error& e) {
//             string err = e.what();
//             if (err.find("expected error") == string::npos) {
//                 // If this is not an expected error, rethrow it
//                 throw;
//             }
//             // If it's an expected error, continue with next action
//             continue;
//         }
//     }
// }

// string citus_distributor::distribute_one(shared_ptr<prod> gen, int* affect_num){
//     auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
//     if(create_table == nullptr){
//         throw runtime_error("not create_table_stmt");
//     }
//     shared_ptr<create_dds> dds = make_shared<citus_create_dds>(create_table->created_table, gen.get());
//     ostringstream s;
//     dds->out(s);
//     auto sql = s.str() + ";";
//     return sql;
// }

// void citus_distributor::clear_record_file(void){
//     //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
//     std::ofstream file(string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE), std::ios::trunc);
//     file.close();
// }

// citus_distributor::citus_distributor(shared_ptr<citus_context> context){
//     this -> context = context;
//     context -> logfile = string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE);
    
//     manager = make_shared<action_sequence_manager>();
//     manager->factory->register_action(make_shared<create_distributed_table>());
//     manager->factory->register_action(make_shared<create_reference_table>());
//     // manager->factory->register_action(make_shared<undistribute_table>());
//     // manager->factory->register_action(make_shared<alter_distributed_table>());
// }

// void citus_rebalancer::rebalance_all(bool need_reset){
//     cout<<"citus_rebalancer rebalance_all"<<endl;
//     if(need_reset) {
//         citus_sql_dut sql_dut(context -> get_dut() -> postgres_dut -> test_db,
//          context -> get_dut() -> postgres_dut -> test_ip,
//           context -> get_dut() -> postgres_dut -> test_port);
//         sql_dut.reset_to_backup();
//     }
//         // initialize the db_schema
//     if(context -> db_schema == nullptr){
//         context -> db_schema = make_shared<schema_citus>(
//             context -> get_dut() -> postgres_dut -> test_db, 
//             context -> get_dut() -> postgres_dut -> test_ip, 
//             context -> get_dut() -> postgres_dut -> test_port, 
//             true);
//     }
//     int sequence_length = 3 + (dx(30) - 1);
    
//     // Generate and execute action sequence
//     for (int i = 0; i < sequence_length; i++) {
//         try {
//             // Randomly pick an action type
//             auto action = manager->factory->random_pick();
//             try{
//                 // First try to fill the action with random parameters
//                 action->random_fill(*context);
//             }catch(const runtime_error& e){
//                 string err = e.what();
//                 if(err.find("No candidates available") != string::npos){
//                     cout << "No more tables to distribute, in rebalancing." << endl;
//                     continue;
//                 }
//                 throw;
//             }
//             // Then try to run it
//             action->run(*context);
//         } catch (const runtime_error& e) {
//             string err = e.what();
//             if (err.find("expected error") == string::npos) {
//                 // If this is not an expected error, rethrow it
//                 throw;
//             }
//             // If it's an expected error, continue with next action
//             continue;
//         }
//     }
//     if(d6() < 3){
//         try{
//             citus_rebalance r;
//             r.random_fill(*context);
//             r.run(*context);
//         }catch(exception& e){
//             string err = e.what();
//             if (err.find("expected error") == string::npos) {
//                 // If this is not an expected error, rethrow it
//                 throw;
//             }
//         }
//     }
// }

// void citus_rebalancer::clear_record_file(void){
//     //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_ALTER_FILE)
//     std::ofstream file(string(CITUS_SAVING_DIR) + string(CITUS_DIST_ALTER_FILE), std::ios::trunc);
//     file.close();
// }

// citus_rebalancer::citus_rebalancer(shared_ptr<citus_context> context){
//     this -> context = context;
//     context -> logfile = string(CITUS_SAVING_DIR) + string(CITUS_DIST_ALTER_FILE);
    
//     manager = make_shared<action_sequence_manager>();
//     manager->factory->register_action(make_shared<create_distributed_table>());
//     manager->factory->register_action(make_shared<create_reference_table>());
//     manager->factory->register_action(make_shared<undistribute_table>());
//     manager->factory->register_action(make_shared<alter_distributed_table>());
//     manager->factory->register_action(make_shared<truncate_local_data_after_distributing_table>());
//     manager->factory->register_action(make_shared<remove_local_tables_from_metadata>());
//     manager->factory->register_action(make_shared<citus_add_local_table_to_metadata>());
//     manager->factory->register_action(make_shared<update_distributed_table_colocation>());
//     manager->factory->register_action(make_shared<citus_move_shard_placement>());
//     auto r = make_shared<citus_rebalance>();
//     // r -> weight = 0.15;
//     manager->factory->register_action(r);
//     manager->factory->register_action(make_shared<citus_rebalance_status>());
//     manager->factory->register_action(make_shared<citus_drain_node>());
//     manager->factory->register_action(make_shared<add_worker_node>());
//     manager->factory->register_action(make_shared<remove_worker_node>());
//     manager->factory->register_action(make_shared<add_secondary_node>());
//     manager->factory->register_action(make_shared<citus_get_active_worker_nodes>());
//     manager->factory->register_action(make_shared<citus_check_cluster_node_health>());
// }

// void create_distributed_table::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT create_distributed_table";
//     vector<table*> undistributed_tables;
//     for(table& c_t : db_schema -> tables){
//         bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
//         bool is_reference = db_schema->reference_tables.count(c_t.name) > 0;
        
//         if(!is_distributed && !is_reference) {
//             undistributed_tables.push_back(&c_t);
//         }
//     }
//     if(undistributed_tables.empty()){
//         throw runtime_error("No candidates available");
//     }
//     table t = *random_pick(undistributed_tables);
//     victim_table_name = t.name;
//     schema_pqxx p(context.get_dut() -> postgres_dut -> test_db, context.get_dut() -> postgres_dut -> test_ip, context.get_dut() -> postgres_dut -> test_port, true);
//     for(table& c_t : p.tables){
//         if(c_t.name == t.name){
//             if(c_t.constraints.empty()){
//                 int col_index = d100() % t.columns().size();
//                 victim_column_name = t.columns()[col_index].name;
//             }
//             else{
//                 victim_column_name = random_pick(c_t.constraints);
//             }
//             break;
//         }
//     }
//     string col_name = victim_column_name;
//     int k = d12();
//     if(k < 3){
//         sql += "('" + t.name + "', '" + col_name + "');";
//     }else if(k < 6 && !db_schema -> distributed_tables.empty()){
//         int index = dx(db_schema -> distributed_tables.size())-1;
//         auto it = db_schema -> distributed_tables.begin();
//         std::advance(it, index);
//         string victim_name = it -> second -> name;
//         sql += "('" + t.name + "', '" + col_name +"', colocate_with:='" + victim_name + "');";
//     }else{
//         sql += "('" + t.name + "', '" + col_name +"', shard_count:=" + to_string(dx(100)) + ");";
//     }
// }

// bool create_distributed_table::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     string sql1;
//     try{
//         context.get_dut() -> test(sql);
//         if(d6()<4){
//             sql1 = "ALTER TABLE " + victim_table_name + " REPLICA IDENTITY FULL;";
//             // cout<<sql1<<endl;
//             context.get_dut() -> test(sql1);
//         }else{
//             sql1 = "ALTER TABLE " + victim_table_name + " ADD CONSTRAINT " + victim_table_name + "_pkey PRIMARY KEY (" + victim_column_name + ");";
//             // cout<<sql1<<endl;
//             context.get_dut() -> test(sql1);
//         }
//     }catch(exception& e){
//         string s = " "+context.get_dut() -> postgres_dut -> test_ip+" "+
//         to_string(context.get_dut() -> postgres_dut -> test_port)+" "
//         +context.get_dut() -> postgres_dut -> test_db;
//         log("#"+sql+s, context.logfile);
//         // log("#"+sql1+s, context.logfile);
//         throw;
//     }
//     //log with ip, port, db
//     string s = " "+context.get_dut() -> postgres_dut -> test_ip+" "+
//         to_string(context.get_dut() -> postgres_dut -> test_port)+" "
//         +context.get_dut() -> postgres_dut -> test_db;
//     log(sql+s, context.logfile);
//     // log(sql1+s, context.logfile);
//     return true;
// }

// void create_reference_table::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT create_reference_table";
//     vector<table*> undistributed_tables;
//     for(table& c_t : db_schema -> tables){
//         bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
//         bool is_reference = db_schema->reference_tables.count(c_t.name) > 0;
        
//         if(!is_distributed && !is_reference) {
//             undistributed_tables.push_back(&c_t);
//         }
//     }
//     if(undistributed_tables.empty()){
//         throw runtime_error("No candidates available");
//     }
//     table t = *random_pick(undistributed_tables);
//     victim_table_name = t.name;
//     sql += "('" + t.name + "');";
// }

// bool create_reference_table::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     string sql1;
//     try{
//         context.get_dut() -> test(sql);
//         if(d6()<4){
//             sql1 = "ALTER TABLE " + victim_table_name + " REPLICA IDENTITY FULL;";
//             // cout<<sql1<<endl;
//             context.get_dut() -> test(sql1);
//         }else{
//             if(d6()<4)
//                 sql1 = "ALTER TABLE " + victim_table_name + " ADD CONSTRAINT " + victim_table_name + "_pkey PRIMARY KEY (pkey);";
//             else
//                 sql1 = "ALTER TABLE " + victim_table_name + " ADD CONSTRAINT " + victim_table_name + "_pkey PRIMARY KEY (vkey);";
//             // cout<<sql1<<endl;
//             context.get_dut() -> test(sql1);
//         }
//     }catch(exception& e){
//         string s = " "+context.get_dut() -> postgres_dut -> test_ip+" "+
//         to_string(context.get_dut() -> postgres_dut -> test_port)+" "
//         +context.get_dut() -> postgres_dut -> test_db;
//         log("#"+sql+s, context.logfile);
//         // log("#"+sql1+s, context.logfile);
//         throw;
//     }
//     //log with ip, port, db
//     string s = " "+context.get_dut() -> postgres_dut -> test_ip+" "+
//         to_string(context.get_dut() -> postgres_dut -> test_port)+" "
//         +context.get_dut() -> postgres_dut -> test_db;
//     log(sql+s, context.logfile);
//     // log(sql1+s, context.logfile);
//     return true;
// }

// void undistribute_table::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT undistribute_table";
//     vector<table*> distributed_tables;
//     for(table& c_t : db_schema -> tables){
//         bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
//         bool is_reference = db_schema->reference_tables.count(c_t.name) > 0;
        
//         if(is_distributed || is_reference) {
//             distributed_tables.push_back(&c_t);
//         }
//     }
//     if(distributed_tables.empty()){
//         throw runtime_error("No candidates available");
//     }
//     table t = *random_pick(distributed_tables);
//     sql += "('" + t.name + "');";
// }

// void alter_distributed_table::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT alter_distributed_table";
//     vector<table*> distributed_tables;
//     for(table& c_t : db_schema -> tables){
//         bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
//         if(is_distributed) {
//             distributed_tables.push_back(&c_t);
//         }
//     }
//     if(distributed_tables.empty()){
//         throw runtime_error("No candidates available");
//     }
//     table t = *random_pick(distributed_tables);
//     int col_index = d100() % t.columns().size();
//     string col_name = t.columns()[col_index].name;
//     int k = d12();
//     if(k < 3){
//         sql += "('" + t.name + "', '" + col_name + "');";
//     }else if(k < 6 && !db_schema -> distributed_tables.empty()){
//         int index = dx(db_schema -> distributed_tables.size())-1;
//         auto it = db_schema -> distributed_tables.begin();
//         std::advance(it, index);
//         string victim_name = it -> second -> name;
//         sql += "('" + t.name + "', '" + col_name +"', colocate_with:='" + victim_name + "');";
//     }else{
//         sql += "('" + t.name + "', '" + col_name +"', shard_count:=" + to_string(dx(100)) + ");";
//     }
// }

// void truncate_local_data_after_distributing_table::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT truncate_local_data_after_distributing_table";
//     vector<table*> distributed_tables;
//     for(table& c_t : db_schema -> tables){
//         bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
//         bool is_reference = db_schema->reference_tables.count(c_t.name) > 0;
        
//         if(is_distributed || is_reference) {
//             distributed_tables.push_back(&c_t);
//         }
//     }
//     if(distributed_tables.empty()){
//         throw runtime_error("No candidates available");
//     }
//     table t = *random_pick(distributed_tables);
//     sql += "('" + t.name + "');";
// }

// void remove_local_tables_from_metadata::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT * from remove_local_tables_from_metadata()";
// }

// void citus_add_local_table_to_metadata::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT citus_add_local_table_to_metadata";
//     vector<table*> undistributed_tables;
//     for(table& c_t : db_schema -> tables){
//         bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
//         bool is_reference = db_schema->reference_tables.count(c_t.name) > 0;
        
//         if(!is_distributed && !is_reference) {
//             undistributed_tables.push_back(&c_t);
//         }
//     }
//     if(undistributed_tables.empty()){
//         throw runtime_error("No candidates available");
//     }
//     table t = *random_pick(undistributed_tables);
//     sql += "('" + t.name + "');";
// }

// void update_distributed_table_colocation::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();
//     sql = "SELECT update_distributed_table_colocation";
//     vector<table*> distributed_tables;
//     for(table& c_t : db_schema -> tables){
//         bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
//         if(is_distributed) {
//             distributed_tables.push_back(&c_t);
//         }
//     }
//     if(distributed_tables.empty()){
//         throw runtime_error("No candidates available");
//     }
//     table t = *random_pick(distributed_tables);
//     int index = dx(db_schema -> distributed_tables.size())-1;
//     auto it = db_schema -> distributed_tables.begin();
//     std::advance(it, index);
//     string victim_name = it -> second -> name;
//     if(d6()<2) victim_name = "none";
//     sql += "('" + t.name + "', colocate_with:='" + victim_name + "');";
// }

// void citus_move_shard_placement::random_fill(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     //lazy refresh
//     shared_ptr<schema_citus> db_schema = context.get_schema();
//     db_schema -> refresh();

//     string select_shard = "SELECT pg_dist_shard.shardid::text, * \
//     FROM pg_dist_shard join citus_shards \
//     on pg_dist_shard.shardid = citus_shards.shardid \
//     where citus_table_type = 'distributed';";
//     vector<row_output> output;
//     vector<row_output*> victims;
//     context.get_dut() -> test(select_shard,&output);
//     for(auto& row: output){
//        //  cout << row[0] <<" " << row[2]<<" "  << row[4]<<" " << row[13] << endl;
//         int min = stoi(row[4]);
//         int max = stoi(row[5]);
//         if((min<=10000 && min >=-10000)||(max<=10000 && max >=-10000)){
//             victims.push_back(&row);
//         }
//         else if(d20() == 10){
//             victims.push_back(&row);
//         }
//     }
//     if(victims.empty()){
//         throw runtime_error("No candidates available");
//     }
//     row_output victim = *random_pick(victims);
//     vector<citus_worker_connection*> connected;
//     //random pick a worker
//     for(auto& w: context.workers){
//         if(w.connected_to_cluster && w.activated  && w.has_database
//         && !(w.test_ip == (victim[11]) && w.test_port == stoi(victim[12]))){
//             connected.push_back(&w);
//         }
//     }
//     if(connected.empty()){
//         throw runtime_error("No candidates available");
//     }
//     auto& worker = random_pick(connected);
//     sql = "SELECT citus_move_shard_placement(" + victim[0] + ", '" 
//     + victim[11] + "', " + victim[12] + ", '" 
//     + worker -> test_ip + "', " + to_string(worker -> test_port) + ");";
// }

// void citus_rebalance::random_fill(Context& ctx){
//     if(d6()<5){
//         int thershold = dx(90) + 9;
//         sql0 = "update pg_dist_rebalance_strategy set improvement_threshold = 0." + to_string(thershold) + " where name = 'by_disk_size';";
//     }else{
//         int thershold = dx(9);
//         sql0 = "update pg_dist_rebalance_strategy set improvement_threshold = 0.0" + to_string(thershold) + " where name = 'by_disk_size';";
//     }
//     sql1 = "SELECT citus_rebalance_start(";
//     if(d6()<2){
//         sql1 += "drain_only:=true, ";
//     }
//     if(d6()<3){
//         sql1 += "rebalance_strategy:='by_shard_count'";
//     }else{
//         sql1 += "rebalance_strategy:='by_disk_size'";
//     }
//     int i = dx(8);
//     if(i<3){
//         sql1 += ", shard_transfer_mode:='auto'";
//     }else if(i<5){
//         sql1 += ", shard_transfer_mode:='block_write'";
//     }else if(i<7){
//         sql1 += ", shard_transfer_mode:='force_logical'";
//     }
//     sql1 += ");";
//     // int timeout = dx(200);
//     // sql2 = "SET statement_timeout = '"+to_string(timeout)+"s';SELECT citus_rebalance_wait();";
//     sql2 = "SET statement_timeout = '200s';SELECT citus_rebalance_wait();";
// }

// bool citus_rebalance::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql0);
//         context.get_dut() -> test(sql1);
//         context.get_dut() -> test(sql2);
//         auto db = context.get_dut() -> postgres_dut -> test_db;
//         auto ip = context.get_dut() -> postgres_dut -> test_ip;
//         auto port = context.get_dut() -> postgres_dut -> test_port;
//         context.get_dut() = make_shared<citus_dut>(db,ip,port);
//     }catch(exception& e){
//         sql = "#"+sql0+"\n#"+sql1+"\n#"+sql2;
//         string s = sql+" "+context.get_dut() -> postgres_dut -> test_ip+" "+
//         to_string(context.get_dut() -> postgres_dut -> test_port)+" "
//         +context.get_dut() -> postgres_dut -> test_db;
//         log(s, context.logfile);
//         throw;
//     }
//     //log with ip, port, db
//     sql = sql0+"\n"+sql1+"\n"+sql2;
//     log(sql+" "+context.get_dut() -> postgres_dut -> test_ip+" "+
//     to_string(context.get_dut() -> postgres_dut -> test_port)+" "
//     +context.get_dut() -> postgres_dut -> test_db,
//     context.logfile);
//     return true;
// }

// void citus_rebalance_status::random_fill(Context& ctx){
//     sql = "SELECT citus_rebalance_status();";
// }

// bool citus_rebalance_status::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         vector<row_output> res;
//         context.get_dut() -> test(sql, &res);
//         if(res.empty()){
//             log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//                 +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//                 + " " + context.get_dut() ->postgres_dut ->test_db
//                 , context.logfile);
//             return true;
//         }
//         for(auto& row: res){
//             if(row.size() == 1){
//                 if(row[0].find("fail") != string::npos || row[0].find("ERROR") != string::npos){
//                     cout<<row[0]<<endl;
//                 }
//                 continue;
//             }
//             else if(row[1].find("fail") != string::npos || row[6].find("ERROR") != string::npos){
//                 cout<<"job id"<<row[0]<<"has failed"<<endl;
//                 // row0 to row 6
//                 string s;
//                 for(int i=0;i<7;i++){
//                     s += row[i] + " ";
//                 }
//                 cout<<s<<endl;
//                 cerr<<s<<endl;
//             }
//         }
//     }catch(exception& e){
//         log("#" + sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//         , context.logfile);
//         throw;
//     }
//     //log query
//     log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//     , context.logfile);
//     return true;
// }

// void citus_drain_node::random_fill(Context& ctx){
//     vector<citus_worker_connection*> connected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(worker.has_database && worker.connected_to_cluster && worker.activated) connected.push_back(&worker);
//     }
//     if(connected.empty()){
//         throw std::runtime_error("expected error: no connected worker found");
//     }
//     auto victim = random_pick(connected);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     // int timeout = dx(200);
//     // sql = "SET statement_timeout = '"+to_string(timeout)+"s';SELECT citus_drain_node('" + test_ip + "', " + to_string(test_port) + ");";
//     sql = "SET statement_timeout = '200s';SELECT citus_drain_node('" + test_ip + "', " + to_string(test_port) + ");";
// }

// bool citus_drain_node::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql);
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.activated = false;
//                 worker.connected_to_cluster = false;
//                 break;
//             }
//         }
//     }
//     catch(exception& e){
//         log("#" + sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//         , context.logfile);
//         throw;
//     }
//     //log query
//     log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//     , context.logfile);
//     return true;
// }
