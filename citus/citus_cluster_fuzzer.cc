#include "citus_cluster_fuzzer.hh"
#include <unistd.h>

void read_citus_worker_config_from_file(std::vector<citus_worker_connection>& workers, std::string db){
    std::ifstream file(CITUS_CONFIGURATION_FILE); 
    if (!file) {
        std::cerr << "cannot open file distribute_expr/citus_config!" << std::endl;
        abort();
    }
    std::string line;
    
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        std::istringstream ss(line);
        std::string ip;
        int port;
        
        if (ss >> ip >> port) {
            workers.push_back(citus_worker_connection(ip,db,port));
        }
    }
    file.close();
}

citus_cluster_fuzzer::citus_cluster_fuzzer(shared_ptr<citus_context> ctx)
: cluster_fuzzer(ctx, CITUS_SAVING_DIR + string(CITUS_CLUSTER_ACTIONS))
{
    base_context = ctx;
    auto context = dynamic_pointer_cast<citus_context>(base_context);
    context -> logfile = CITUS_SAVING_DIR + string(CITUS_CLUSTER_ACTIONS);
    // context -> master_dut = make_shared<citus_dut>(db, ip, port);
    if(context -> workers.empty()){
        context -> workers = vector<citus_worker_connection>();
        read_citus_worker_config_from_file(context -> workers, context -> info.test_db);
    }

}

void citus_cluster_fuzzer::reset_init(){
    auto context = dynamic_pointer_cast<citus_context>(base_context);
    context -> master_dut -> reset();
    for(auto& worker:context -> workers){
        //may have residual connections
        while(true){
            try{
                worker.dut -> reset();
                worker.has_database = true;
            }catch(exception& e){
                sleep(6);
                cout<<"retry drop database in " << worker.test_ip <<": "<< to_string(worker.test_port)<<endl;
                continue;
            }
            break;
        }
    }
    //for each worker, execute add_worker_node
    std::ifstream ifile("./citus/citus_docker_config");
    if (!ifile.is_open()) {
        throw std::runtime_error("failed to open config file: /citus/citus_docker_config.");
    }
    int i = 1;
    for(auto& worker:context -> workers){
        string test_ip="";
        std::getline(ifile, test_ip);
        string test_port = "5432";
        string sql = "select * from citus_add_node('"+ test_ip + "', " + test_port + ");";
        i++;
        context -> master_dut -> test(sql);
    }

    std::ofstream ofile(CITUS_SAVING_DIR + string(CITUS_CLUSTER_ACTIONS), std::ios::trunc);
    ofile.close();
}

// action_sequence citus_cluster_config_fuzzer::mutate_and_run(action_sequence& victim) {
//     // auto mutated_actions = mutate_and_run_template(victim);
//     // //check if the cluster is healthy
//     // citus_get_active_worker_nodes a;
//     // a.random_fill(*context);
//     // a.run(*context);
//     // return mutated_actions;
//     return victim;
// }

// citus_cluster_fuzzer::citus_cluster_fuzzer(string db, string ip, unsigned int port, shared_ptr<citus_context> ctx)
// {
//     ctx->logfile = CITUS_SAVING_DIR + string(CITUS_CLUSTER_ACTIONS);
//     fuzzers.push_back(make_shared<citus_cluster_config_fuzzer>(db,ip,port,ctx));
// }

// void citus_cluster_fuzzer::ruin(){
//     //do nothing, because citus_cluster_config_fuzzer will reset_init in fuzz
//     // citus_cluster_config_fuzzer& fuzzer = dynamic_cast<citus_cluster_config_fuzzer&>(*fuzzers[0]);
//     // fuzzer.reset_init();
// }

// add_worker_node::add_worker_node(citus_worker_connection& c)
// {
//     test_ip = c.test_ip;
//     test_port = c.test_port;
//     sql = "select * from citus_add_node('"+ test_ip + "', " + to_string(test_port) + ");";
// }

// bool add_worker_node::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql);
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.connected_to_cluster = true;
//                 worker.has_database = true;
//                 worker.activated = true;
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
//         , context.logfile);
//     return true;
// }

// void add_worker_node::random_fill(Context& ctx){
//     vector<citus_worker_connection*> unconnected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(!worker.connected_to_cluster) unconnected.push_back(&worker);
//     }
//     if(unconnected.empty()){
//         throw std::runtime_error("expected error: no unconnected worker found");
//     }
//     auto victim = random_pick(unconnected);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     sql = "select * from citus_add_node('"+ test_ip + "', " + to_string(test_port) + ");";
// }

// remove_worker_node::remove_worker_node(citus_worker_connection& c)
// {
//     test_ip = c.test_ip;
//     test_port = c.test_port;
//     sql = "select * from citus_remove_node('"+ test_ip + "', " + to_string(test_port) + ");";
// }

// bool remove_worker_node::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql);
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.activated = false;
//                 worker.connected_to_cluster = false;
//             }
//         }
//     }catch(exception& e){ 
//         //log with ip and port and db
//         log("#" + sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db,
//          context.logfile);
//         throw;
//     }
//     //log query
//     log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//     , context.logfile);
//     return true;
// }

// void remove_worker_node::random_fill(Context& ctx){
//     vector<citus_worker_connection*> connected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(worker.connected_to_cluster) connected.push_back(&worker);
//     }
//     if(connected.empty()){
//         throw std::runtime_error("expected error: no connected worker found");
//     }
//     auto victim = random_pick(connected);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     sql = "select * from citus_remove_node('"+ test_ip + "', " + to_string(test_port) + ");";
// }

// bool add_secondary_node::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql);
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.connected_to_cluster = true;
//                 worker.activated = true;
//                 worker.has_database = true;
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

// void add_secondary_node::random_fill(Context& ctx){
//     vector<citus_worker_connection*> unconnected;
//     vector<citus_worker_connection*> connected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(!worker.connected_to_cluster) unconnected.push_back(&worker);
//         else connected.push_back(&worker);
//     }
//     if(unconnected.empty()){
//         throw std::runtime_error("expected error: no unconnected worker found");
//     }
//     auto victim = random_pick(unconnected);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     auto primary = random_pick(connected);
//     primary_ip = primary -> test_ip;
//     primary_port = primary -> test_port;
//     sql = "select * from citus_add_secondary_node('"+ test_ip + "', " + to_string(test_port) + ", '" + primary_ip + "', " + to_string(primary_port) + ");";
// }

// bool drop_worker_database::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.dut = make_shared<citus_dut>("postgres", worker.test_ip, worker.test_port);
//                 worker.dut -> test(sql);
//                 worker.has_database = false;
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

// void drop_worker_database::random_fill(Context& ctx){
//     vector<citus_worker_connection*> connected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(worker.has_database && worker.connected_to_cluster) connected.push_back(&worker);
//     }
//     if(connected.empty()){
//         throw std::runtime_error("expected error: no connected worker found");
//     }
//     auto victim = random_pick(connected);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     sql = "drop database if exists " + victim -> test_db + " with (force);";
// }

// bool create_worker_database::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.dut = make_shared<citus_dut>("postgres", worker.test_ip, worker.test_port);
//                 worker.dut -> test(sql1);
//                 worker.dut = make_shared<citus_dut>(worker.test_db, worker.test_ip, worker.test_port);
//                 worker.dut -> test(sql2);
//                 worker.has_database = true;
//                 break;
//             }
//         }
//     }
//     catch(exception& e){
//         sql = "# "+sql1+"\n# "+sql2;
//         log( sql +" "+context.get_dut() ->postgres_dut ->test_ip 
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

// void create_worker_database::random_fill(Context& ctx){
//     vector<citus_worker_connection*> unconnected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(!worker.has_database && worker.connected_to_cluster) unconnected.push_back(&worker);
//     }
//     if(unconnected.empty()){
//         throw std::runtime_error("expected error: no unconnected worker found");
//     }
//     auto victim = random_pick(unconnected);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     sql1 = "create database " + victim -> test_db + ";";
//     sql2 = "create extension citus;";
//     sql = sql1 + "\n" + sql2;
// }

// bool recreate_worker_database::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.dut = make_shared<citus_dut>("postgres", worker.test_ip, worker.test_port);
//                 worker.dut -> test(sql1);
//                 worker.dut -> test(sql2);
//                 worker.dut = make_shared<citus_dut>(worker.test_db, worker.test_ip, worker.test_port);
//                 worker.dut -> test(sql3);
//                 worker.has_database = true;
//                 worker.activated = false;
//                 worker.connected_to_cluster = false;
//                 break;
//             }
//         }
//     }
//     catch(exception& e){
//         sql = "# "+sql1+"\n# "+sql2+"\n# "+sql3;
//         log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
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

// void recreate_worker_database::random_fill(Context& ctx){
//     vector<citus_worker_connection*> connected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(worker.has_database && worker.connected_to_cluster) connected.push_back(&worker);
//     }
//     if(connected.empty()){
//         throw std::runtime_error("expected error: no connected worker found");
//     }
//     auto victim = random_pick(connected);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     sql1 = "drop database if exists " + victim -> test_db + " with (force);";
//     sql2 = "create database " + victim -> test_db + ";";
//     sql3 = "create extension citus;";
//     sql = sql1 + "\n" + sql2 + "\n" + sql3;
// }

// bool citus_get_active_worker_nodes::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         vector<row_output> output;
//         context.get_dut() -> test(sql, &output);
//         for(auto& row:output){
//             cout << "citus_get_active_worker_nodes: " << row[0] << " " << row[1] << endl;
//         }
//         for(auto& worker : context.workers){
//             bool found = false;
//             for(auto& row:output){
//                 string ip = row[0];
//                 unsigned int port = stoi(row[1]);
//                 if(worker.test_ip == ip && worker.test_port == port){
//                     found = true;
//                     worker.activated = true;
//                     worker.has_database = true;
//                     worker.connected_to_cluster = true;
//                     break;
//                 }
//             }
//             if(!found){
//                 worker.activated = false;
//                 worker.connected_to_cluster = false;
//             }
//         }
//     }catch(exception& e){
//         log("#" + sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//         , context.logfile);
//         cout<< "citus_get_active_worker_nodes failed"<<endl;
//         abort();
//     }
//     log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//     , context.logfile);
//     return true;
// }

// void citus_get_active_worker_nodes::random_fill(Context& ctx){
//     sql = "select * from citus_get_active_worker_nodes();";
// }

// bool citus_check_cluster_node_health::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql);
//     }catch(exception& e){
//         log("#" + sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//         , context.logfile);
//         cout<< "citus_check_cluster_node_health failed"<<endl;
//         abort();
//     }
//     log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//     , context.logfile);
//     return true;
// }

// void citus_check_cluster_node_health::random_fill(Context& ctx){
//     sql = "SELECT * FROM citus_check_cluster_node_health();";
// }

// void citus_set_coordinator_host::random_fill(Context& ctx){
//     vector<citus_worker_connection*> connected;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(worker.connected_to_cluster) connected.push_back(&worker);
//     }
//     if(connected.empty()){
//         throw std::runtime_error("expected error: no connected worker found");
//     }
//     auto victim = random_pick(connected);
//     worker_test_ip_to_be_coordinator = victim -> test_ip;
//     worker_test_port_to_be_coordinator = victim -> test_port;
//     sql = "SELECT citus_set_coordinator_host('"+ worker_test_ip_to_be_coordinator + "', " + to_string(worker_test_port_to_be_coordinator) + ");";
// }

// bool citus_set_coordinator_host::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     string old_master_dut_ip = context.get_dut() -> postgres_dut -> test_ip;
//     unsigned int old_master_dut_port = context.get_dut() -> postgres_dut -> test_port;
//     string old_master_dut_db = context.get_dut() -> postgres_dut -> test_db;
//     try{
//         context.get_dut() -> test(sql);
//         for(auto& worker:context.workers){
//             if(worker.test_ip == worker_test_ip_to_be_coordinator && worker.test_port == worker_test_port_to_be_coordinator)
//             {
//                 auto new_master_dut = worker.dut;
//                 worker.dut = context.get_dut();
//                 worker.test_ip = context.get_dut() -> postgres_dut -> test_ip;
//                 worker.test_port = context.get_dut() -> postgres_dut -> test_port;
//                 worker.test_db = context.get_dut() -> postgres_dut -> test_db;
//                 worker.connected_to_cluster = true;
//                 worker.has_database = true;
//                 context.get_dut() = new_master_dut;
//                 break;
//             }
//         }
//     }catch(exception& e){ 
//         //log with ip and port and db
//         log("#" + sql +" "+ old_master_dut_ip 
//         +" "+ to_string(old_master_dut_port)
//         + " " + old_master_dut_db,
//          context.logfile);
//         throw;
//     }
//     //log query
//     log(sql +" "+ old_master_dut_ip 
//         +" "+ to_string(old_master_dut_port)
//         + " " + old_master_dut_db
//     , context.logfile);
//     return true;
// }

// bool citus_activate_node::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql);
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.activated = true;
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
//         , context.logfile);
//     return true;
// }

// void citus_activate_node::random_fill(Context& ctx){
//     vector<citus_worker_connection*> unactivated;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(!worker.activated && worker.connected_to_cluster && worker.has_database) 
//             unactivated.push_back(&worker);
//     }
//     if(unactivated.empty()){
//         throw std::runtime_error("expected error: no unactivated worker found");
//     }
//     auto victim = random_pick(unactivated);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     sql = "select * from citus_activate_node('"+ test_ip + "', " + to_string(test_port) + ");";
// }


// bool citus_disable_node::run(Context& ctx){
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     try{
//         context.get_dut() -> test(sql);
//         for(auto& worker:context.workers){
//             if(worker.test_ip == test_ip && worker.test_port == test_port){
//                 worker.activated = false;
//                 if(d6()<3) worker.connected_to_cluster = false;
//                 else worker.connected_to_cluster = true;
//             }
//         }
//     }catch(exception& e){ 
//         //log with ip and port and db
//         log("#" + sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db,
//          context.logfile);
//         throw;
//     }
//     //log query
//     log(sql +" "+context.get_dut() ->postgres_dut ->test_ip 
//         +" "+ to_string(context.get_dut() ->postgres_dut ->test_port)
//         + " " + context.get_dut() ->postgres_dut ->test_db
//     , context.logfile);
//     return true;
// }

// void citus_disable_node::random_fill(Context& ctx){
//     vector<citus_worker_connection*> activated;
//     auto& context = dynamic_cast<citus_context&>(ctx);
//     for(auto& worker:context.workers){
//         if(worker.activated && worker.connected_to_cluster && worker.has_database) 
//             activated.push_back(&worker);
//     }
//     if(activated.empty()){
//         throw std::runtime_error("expected error: no activated worker found");
//     }
//     auto victim = random_pick(activated);
//     test_ip = victim -> test_ip;
//     test_port = victim -> test_port;
//     sql = "select * from citus_disable_node('"+ test_ip + "', " + to_string(test_port) + ");";
// }