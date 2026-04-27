#include "ss_cluster_fuzzer.hh"
#include "ss_action.hh"
#include <unistd.h>

void read_ss_worker_config_from_file(std::vector<ss_worker_connection>& workers, std::string db,string single_dbms_name){
    string config_file;
    if(single_dbms_name == "postgres"){
        config_file = SS_PG_CONFIGURATION_FILE;
    }
    else if(single_dbms_name == "mysql"){
        config_file = SS_MYSQL_CONFIGURATION_FILE;
    }
    else{
        std::cerr << "cannot open file shardingsphere/" << config_file << "!" << std::endl;
        abort();
    }
    std::ifstream file(config_file); 
    std::string line;
    int id = 0;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        std::istringstream ss(line);
        std::string ip;
        int port;
        string user;
        string password;
        if (ss >> ip >> port >> user >> password) {
            workers.push_back(ss_worker_connection(ip,db,port,single_dbms_name,id,user,password));
            id++;
        }
    }
    file.close();
}

ss_cluster_fuzzer::ss_cluster_fuzzer(shared_ptr<ss_context> ctx)
: cluster_fuzzer(ctx, SS_SAVING_DIR + string(SS_CLUSTER_ACTIONS)){
    base_context = ctx;
    auto context = dynamic_pointer_cast<ss_context>(base_context);
    context -> logfile = SS_SAVING_DIR + string(SS_CLUSTER_ACTIONS);
    // context -> master_dut = make_shared<citus_dut>(db, ip, port);
    if(context -> workers.empty()){
        context -> workers = vector<ss_worker_connection>();
        read_ss_worker_config_from_file(context -> workers, context -> info.test_db, context -> single_dbms_name);
    }
}

void ss_cluster_fuzzer::reset_init(){
    auto context = dynamic_pointer_cast<ss_context>(base_context);
    // context -> master_dut -> reset();
    if(context -> single_dbms_name == "postgres"){
        auto dut1 = make_shared<dut_libpq>("postgres",context -> info.test_ip, context -> info.test_port);
        dut1 -> test("DROP DATABASE IF EXISTS " + context -> info.test_db +";");
        cout<<"drop database in ss_master"<<endl;
    }

    for(auto& worker:context -> workers){
        //may have residual connections
        while(true){
            try{
                worker.dut -> reset();
                cout<<"reset database in ss_worker"<< worker.test_port <<endl;
                // worker.has_database = true;
            }catch(exception& e){
                sleep(6);
                cout<<"retry drop database in " << worker.test_ip <<": "<< to_string(worker.test_port)<<endl;
                continue;
            }
            break;
        }
    }

    if(context -> single_dbms_name == "postgres"){
        auto dut1 = make_shared<dut_libpq>("postgres",context -> info.test_ip, context -> info.test_port);
        dut1 -> test("CREATE DATABASE " + context -> info.test_db +";");
        cout<<"create database in ss_master"<<endl;
        // context -> master_dut = make_shared<ss_sql_dut>();
        sleep(10);
    }

    //for each worker, execute REGISTER STORAGE UNIT
    int i = 0;
    for(auto& worker:context -> workers){
        while(true){
            try{
                string sql = "REGISTER STORAGE UNIT " + worker.storage_unit + " ("
                    + "HOST=\"" + worker.test_ip + "\","
                    + "PORT=" + to_string(worker.test_port) + ","
                    + "DB=\"" + worker.test_db + "\","
                    + "USER=\"" + worker.user + "\","
                    + "PASSWORD=\"" + worker.password + "\");";
                i++;
                context -> master_dut -> test(sql);
                break;
            }catch(exception& e){
                sleep(6);
                cout<<"retry reset in " << worker.test_ip <<": "<< to_string(worker.test_port)<<endl;
                continue;
            }
        }
    }

    std::ofstream ofile(SS_SAVING_DIR + string(SS_CLUSTER_ACTIONS), std::ios::trunc);
    ofile.close();
}

// void ss_cluster_config_fuzzer::reset_init(){
//     cout<<"ss reset"<<endl;
//     if(context -> single_dbms_name == "postgres"){
//         string testdb =  context -> get_dut() -> db;
//         context -> get_dut() -> dut = make_shared<dut_libpq>("postgres",context -> get_dut() -> ip, context -> get_dut() -> port);
//         context -> get_dut() -> test("DROP DATABASE IF EXISTS " + testdb +";");

//         for(auto worker : context -> workers){
//             while(true){
//                 try{
//                     worker -> dut -> reset();
//                     break;
//                 }catch(exception& e){
//                     sleep(6);
//                     cout<<"retry reset in " << worker -> test_ip <<": "<< to_string(worker -> test_port)<<endl;
//                     continue;
//                 }
//                 break;
//             }
//         }
//         volatile auto schema_worker = make_shared<schema_pqxx>(testdb, context -> workers[0] -> test_ip,
//              context -> workers[0] -> test_port, false);
//         //create database on ss master
//         context -> get_dut() -> dut -> test("CREATE DATABASE " + testdb +";");
//         cout<<"create database " + testdb + " on ss master"<<endl;
//         sleep(6);
//         context -> get_dut() -> dut = make_shared<dut_libpq>(testdb ,context -> get_dut() -> ip, context -> get_dut() -> port);
//     }
// #ifdef HAVE_MYSQL
//     else if(context -> single_dbms_name == "mysql"){
        
//     }
// #endif

//     // //register 3 storage unit
//     // for(int i = 0; i < 3; i++){
//     //     auto init_action = make_shared<register_storage_unit>(context -> workers[i]);
//     //     cout<<"register storage unit: "<< init_action -> sql <<endl;
//     //     init_action -> run(*context);
//     // }
//     auto init_action = make_shared<register_storage_unit>(context -> workers[0]);
//     cout<<"register storage unit: "<< init_action -> sql <<endl;
//     init_action -> run(*context);

//     //clear file
//     std::ofstream ofile(SS_SAVING_DIR + string(SS_CLUSTER_ACTIONS), std::ios::trunc);
//     ofile.close();
// }

// action_sequence ss_cluster_config_fuzzer::mutate_and_run(action_sequence& victim){
//     return mutate_and_run_template(victim);
// }

// ss_cluster_config_fuzzer::ss_cluster_config_fuzzer(string db, string ip, unsigned int port, shared_ptr<ss_context> ctx){
//     context = ctx;
//     base_context = ctx;
//     context -> logfile = SS_SAVING_DIR + string(SS_CLUSTER_ACTIONS);
//     context -> master_dut = make_shared<ss_dut>(db, ip, port, context -> single_dbms_name);
//     if(context -> workers.empty()){
//         context -> workers = vector<shared_ptr<ss_worker_connection>>();
//         read_ss_worker_config_from_file(context -> workers, db, context -> single_dbms_name);
//     }
//     name = "ss_cluster_config_fuzzer";
//     action_sequence actions0;
//     actions0.push_back(make_shared<register_storage_unit>(context -> workers[context -> workers.size()-1]));
//     actions0.push_back(make_shared<register_storage_unit>(context -> workers[context -> workers.size()-2]));

//     vector<action_sequence> seed_templates;
//     seed_templates.push_back(actions0);
//     auto seed_template = random_pick(seed_templates);
//     mutator = make_shared<action_sequence_mutator>(seed_template);
//     //register actions
//     mutator->factory -> register_action(make_shared<register_storage_unit>());
//     mutator->factory -> register_action(make_shared<unregister_storage_unit>());
// }

// void ss_cluster_fuzzer::ruin(){
// }

// ss_cluster_fuzzer::ss_cluster_fuzzer(string db, string ip, unsigned int port, shared_ptr<ss_context> ctx){
//     context = ctx;
//     ctx->logfile = SS_SAVING_DIR + string(SS_CLUSTER_ACTIONS);
//     fuzzers.push_back(make_shared<ss_cluster_config_fuzzer>(db,ip,port,ctx));
// }