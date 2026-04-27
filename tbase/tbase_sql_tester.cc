// #include "tbase_sql_tester.hh"
// #include <unistd.h>

// void tbase_sql_dut::reset_to_backup(void){
//     while(true){
//         try{
//             string sql = "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();";
//             postgres_dut -> test(sql);
//             postgres_dut -> reset_without_force();
//             break;
//         }catch(exception& e){
//             // do nothing
//             cerr << "tbase_sql_dut::reset_to_backup" << e.what() << endl;
//             sleep(6);
//         }
//     }

//     //read from the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
//     ifstream file(string(TBASE_SAVING_DIR) + string(TBASE_DIST_RECORD_FILE));
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
//         postgres_dut -> test(stmt);
//     }
//     postgres_dut -> load_single_backup_data();
// }



// string tbase_distributor::distribute_one(shared_ptr<prod> gen , int* affect_num){
//     auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
//     if(create_table == nullptr){
//         throw runtime_error("not create_table_stmt");
//     }
//     context -> temp_table = create_table;
//     auto action = manager->factory->random_pick();
//     action->random_fill(*context);
//     auto dist_action = dynamic_cast<tbase_distribute_action*>(action.get());
//     if (!dist_action) {
//         throw runtime_error("Invalid action type");
//     }
//     dist_action->run(*context, affect_num);
//     stringstream ss;
//     action->out(ss);
//     return ss.str();
// }

// void tbase_distributor::clear_record_file(void){
//     //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
//     std::ofstream file(string(TBASE_SAVING_DIR) + string(TBASE_DIST_RECORD_FILE), std::ios::trunc);
//     file.close();
// }

// tbase_distributor::tbase_distributor(shared_ptr<tbase_context> context){
//     this->context = context;
//     context -> logfile = string(TBASE_SAVING_DIR) + string(TBASE_DIST_RECORD_FILE);
//     manager = make_shared<action_sequence_manager>();
//     manager -> factory -> register_action(make_shared<distribute_by_shard>());
//     manager -> factory -> register_action(make_shared<distribute_by_replication>());
// }

// void distribute_by_shard::random_fill(Context& ctx){
//     auto& context = dynamic_cast<tbase_context&>(ctx);
//     create_table = context.temp_table;
//     auto columns_in_table = create_table -> created_table -> columns();
//     dist_column = &random_pick(columns_in_table);

//     sql = "distribute by shard(" + dist_column -> name + ");";
//     cout << "distribute_by_shard::random_fill"<<sql << endl;
// }

// bool tbase_distribute_action::run(Context& ctx, int* affect_num){
//     auto& context = dynamic_cast<tbase_context&>(ctx);
//     string full_sql;
//     try{
//         ostringstream s;
//         create_table->out(s);
//         auto create_table_str = s.str();
//         full_sql = create_table_str + "\n" + sql;
//         context.master_dut -> test(full_sql, NULL, affect_num);
//     }catch(exception& e){
//         // do nothing
//         throw;
//     }
//     log(full_sql, context.logfile);
//     return true;
// }

// void distribute_by_replication::random_fill(Context& ctx){
//     auto& context = dynamic_cast<tbase_context&>(ctx);
//     create_table = context.temp_table;
//     string group_name = "default_group";
//     sql = "distribute by replication;";
//     cout << "distribute_by_replication::random_fill"<<sql << endl;
// }

