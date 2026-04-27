#include "ss_sql_tester.hh"
#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

void ss_sql_dut::reset(){
    if(single_db_name == "postgres"){
        cout<<"ss_sql_dut reset"<<endl;
        auto dut1 = make_shared<dut_libpq>("postgres", ip, port);
        dut1 -> test("DROP DATABASE IF EXISTS " + db +";");
        dut1 -> test("CREATE DATABASE " + db +";");
        shared_ptr<ss_context> context = make_shared<ss_context>();
        // ss_dut::reset();
    }
}

static inline void trim_inplace(std::string& s) {
    auto not_space = [](int ch){ return !std::isspace(ch); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
    s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
}

vector<string> ss_sql_dut::get_query_plan(const std::vector<std::vector<std::string>>& query_plan) {
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

// void ss_sql_dut::reset(){
//     cout<<"ss sql reset"<<endl;
//     shared_ptr<ss_context> context = make_shared<ss_context>();
//     context -> single_dbms_name = single_db_name;
//     ss_cluster_config_fuzzer fuzzer(db, ip ,port,context);
//     fuzzer.reset_init();
// }

// void ss_sql_dut::reset_to_backup_without_data(){
//     cout<<"ss reset_to_backup"<<endl;
//     reset();
//     //read from the record file string(SS_SAVING_DIR) + string(SS_CLUSTER_ACTIONS)
//     ifstream file(string(SS_SAVING_DIR) + string(SS_CLUSTER_ACTIONS));
//     string line;
//     string current_stmt;
//     vector<string> stmts;
//     while (getline(file, line)) {
//         current_stmt += line + "\n";
//         for (char c : line) {
//             if (c == ';') {
//                 stmts.push_back(current_stmt);
//                 current_stmt = "";
//             }
//         }
//     }
//     file.close();

//     for (string stmt : stmts) {
//         if(stmt[0]!='#'){
//             test(stmt);
//             log_query(stmt);
//             log(stmt+"\n",string(SS_SAVING_DIR) + string(SS_CLUSTER_ACTIONS));
//         }
//     }
// }

// void ss_sql_dut::reset_to_backup(){
//     reset_to_backup_without_data();
//     if(single_dbms_name == "postgres"){
//         auto postgres_dut = dynamic_pointer_cast<dut_libpq>(dut);
//         postgres_dut -> load_single_backup_data();
//     }
// #ifdef HAVE_MYSQL
//     else if(single_dbms_name == "mysql"){
//         auto mysql_dut = dynamic_pointer_cast<dut_mysql>(dut);
//         mysql_dut -> load_single_backup_data();
//     }
// #endif
// }

// void ss_distributor::distribute_all(void){
//     cout<<"ss distributor distribute_all"<<endl;
//     // initialize the ss_schema
//     if(context -> db_schema == nullptr){
//         context -> db_schema = make_shared<ss_schema>(
//             context -> get_dut() -> db,
//             context -> get_dut() -> ip, 
//             context -> get_dut() -> port, 
//             context -> single_dbms_name);
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

// void ss_distributor::clear_record_file(void){
//     //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
//     std::ofstream file(string(SS_SAVING_DIR) + string(SS_DIST_RECORD_FILE), std::ios::trunc);
//     file.close();
// }   

// ss_distributor::ss_distributor(shared_ptr<ss_context> context){
//     this -> context = context;
//     context -> logfile = string(SS_SAVING_DIR) + string(SS_DIST_RECORD_FILE);
    
//     manager = make_shared<action_sequence_manager>();
//     manager->factory->register_action(make_shared<create_sharding_table_rule>());
//     manager->factory->register_action(make_shared<create_broadcast_table_rule>());
//     manager->factory->register_action(make_shared<load_single_table>());
// }

// std::string ss_distributor::distribute_one(shared_ptr<prod> gen, int* affect_num){
//     auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
//     if(create_table == nullptr){
//         throw runtime_error("not create_table_stmt");
//     }
//     context -> temp_table = create_table;
//     auto action = manager->factory->random_pick();
//     action->random_fill(*context);
//     auto dist_action = dynamic_cast<ss_distribute_action*>(action.get());
//     if (!dist_action) {
//         throw runtime_error("Invalid action type");
//     }
//     dist_action->run(*context, affect_num);
//     stringstream ss;
//     action->out(ss);
//     return ss.str();
// }

// void ss_rebalancer::rebalance_all(bool need_reset){
//     cout<<"ss rebalancer rebalance_all"<<endl;
//     if(need_reset) {
//         ss_sql_dut sql_dut(context -> get_dut() -> db,
//          context -> get_dut() -> ip,
//           context -> get_dut() -> port,
//           context -> single_dbms_name);
//         sql_dut.reset_to_backup_without_data();
//     }
//     // initialize the ss_schema
//     if(context -> db_schema == nullptr){
//         context -> db_schema = make_shared<ss_schema>(
//             context -> get_dut() -> db,
//             context -> get_dut() -> ip, 
//             context -> get_dut() -> port, 
//             context -> single_dbms_name);
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
// }

// void ss_rebalancer::clear_record_file(void){
//     //clear the record file string(CITUS_SAVING_DIR) + string(SS_DIST_ALTER_FILE)
//     std::ofstream file(string(SS_SAVING_DIR) + string(SS_DIST_ALTER_FILE), std::ios::trunc);
//     file.close();
// }

// ss_rebalancer::ss_rebalancer(shared_ptr<ss_context> context){
//     this -> context = context;
//     context -> logfile = string(SS_SAVING_DIR) + string(SS_DIST_ALTER_FILE);
    
//     manager = make_shared<action_sequence_manager>();
//     manager->factory->register_action(make_shared<alter_sharding_table_rule>());
// }

