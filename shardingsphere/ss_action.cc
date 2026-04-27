#include "ss_action.hh"
#include "ss.hh"
#include <unistd.h>

bool ss_create_dds::run(Context& ctx){
    auto& context = dynamic_cast<ss_context&>(ctx);
    stringstream ss;
    this->out(ss);
    string sql = ss.str();
    try{
        context.master_dut -> test(sql);
        // impact(context);
    }catch(exception& e){
        // string s = "#"+sql+" "+context.master_dut -> ip+" "+
        // to_string(context.master_dut -> port)+" "
        // +context.master_dut ->db;
        log("#"+sql, context.logfile);
        throw;
    }
    //log without ip, port, db
    log(sql, context.logfile);
    return true;
}

create_broadcast_table_rule::create_broadcast_table_rule(shared_ptr<table> victim_table, prod *parent)
:ss_create_dds(victim_table,parent){
    if(victim_table != nullptr){
        victim_table_name = victim_table -> name;
        return;
    }
    throw runtime_error("we expect a victim table for create_reference_table()");
}

void create_broadcast_table_rule::out(std::ostream &out){
    out<<"CREATE BROADCAST TABLE RULE "<<victim_table_name<<";";
}

create_single_table_rule::create_single_table_rule(shared_ptr<table> victim_table, string storage_unit, prod *parent)
:ss_create_dds(victim_table,parent){
    if(storage_unit != ""){
        this -> storage_unit = storage_unit;
    }
    if(victim_table != nullptr){
        victim_table_name = victim_table -> name;
        return;
    }
    throw runtime_error("we expect a victim table for create_reference_table()");
}

void create_single_table_rule::out2(std::ostream &out){
    out<<"SET DEFAULT SINGLE TABLE STORAGE UNIT = "<<storage_unit<<";";
}

void create_single_table_rule::out(std::ostream &out){
   // out<<"LOAD SINGLE TABLE "<<storage_unit<<"."<<victim_table_name<<";";
   out<<"SET DEFAULT SINGLE TABLE STORAGE UNIT = "<<storage_unit<<";";
}

create_sharding_table_rule::create_sharding_table_rule(shared_ptr<table> victim_table,vector<string> storage_units, string strategy
        , shared_ptr<column> d_key, int shard_cnt, prod *parent)
:ss_create_dds(victim_table,parent){
    use_colocate = false;
    ss_schema* db_schema = dynamic_cast<ss_schema*>(parent->scope->schema);
    if(storage_units.empty()) throw runtime_error("storage units not found");
    this-> storage_units = storage_units;
    if(victim_table == nullptr){
        throw runtime_error("ShardingSphere needs a rule with a victim table before creating this table.");
    }else{
        victim = victim_table;
    }

    if(d_key == nullptr){
        column col = random_pick(victim -> columns());
        this -> dkey = make_shared<column>(col);
    }else{
        this -> dkey = d_key;
    }

    if(shard_cnt == -1){
        if(d6() < 2){
            this -> shard_num = dx(4) + 1;
        }
        else{
            this -> shard_num = dx(20) + 5;
        }
    }
    else this -> shard_num = shard_cnt;

    if(strategy == ""){
        // vector<string> strategy_list = {"MOD","HASH_MOD","VOLUME_RANGE","BOUNDARY_RANGE"};
        vector<string> strategy_list = {"HASH_MOD"};
        if(this -> dkey->type == parent->scope->schema->inttype){
            strategy_list.push_back("MOD");
            strategy_list.push_back("VOLUME_RANGE");
            strategy_list.push_back("BOUNDARY_RANGE");
        }else if(this -> dkey->type == parent->scope->schema->realtype){
            strategy_list.push_back("MOD");
        }else if(this -> dkey->type == parent->scope->schema->datetype){
            strategy_list.push_back("AUTO_INTERVAL");
        }else if(this -> dkey->type == parent->scope->schema->texttype){
            // do nothing
        }
        this->strategy = random_pick(strategy_list);
    }else{
        this->strategy = strategy;
    }
}

void create_sharding_table_rule::out(std::ostream &out){
    out<<"CREATE SHARDING TABLE RULE "<<victim -> name<<" (\n";
    out<<"STORAGE_UNITS(";
    int unit_num = storage_units.size();
    int i = 0;
    for(string& s:storage_units){
        out<<s;
        if(i != unit_num-1){
            out<<",";
        }
        i++;
    }
    out<<"),\n";
    out<<"SHARDING_COLUMN="<<dkey->name<<",TYPE(NAME=\""<< strategy <<"\",PROPERTIES(\n";
    if(strategy == "HASH_MOD"|| strategy == "MOD"){
        out<<"\"sharding-count\"=\""<<to_string(shard_num)<<"\"";
    }else if(strategy == "VOLUME_RANGE"){
        int64_t lower = -2147483648;
        if(d6()==1){
            lower = -150 + d100();
        }
        int64_t upper = 2147483647;
        if(d6()==1){
            upper = 50 + d100();
        }
        out<<"\"range-lower\"=\"";
        out << lower;
        out<<"\",\n";

        out<<"\"range-upper\"=\"";
        out << upper;
        out<<"\",";

        out<<"\"sharding-volume\"=\"";
        int64_t vol = (upper-lower+1) / shard_num;
        out << vol;
        out<<"\"";
    }else if(strategy == "BOUNDARY_RANGE"){
        int64_t lower = -2147483648;
        if(d6()==1){
            lower = -150 + d100();
        }
        int64_t upper = 2147483647;
        if(d6()==1){
            upper = 50 + d100();
        }
        set<int64_t> boundary;
        size_t need = (shard_num > 1) ? shard_num : 2;
        int64_t domain = upper - lower + 1;

        while (boundary.size() < need) boundary.insert(rand_int64(lower, upper));

        out << "\"sharding-ranges\"" << "=\"";
        size_t i = 0;
        for (auto b : boundary) {
            out << b;
            if (++i != boundary.size()) out << ",";
        }
        out << "\"";

    }else if(strategy == "AUTO_INTERVAL"){
        int64_t lower = 1970 + d20();
        int64_t upper = 2107 - d20();
        size_t need = (shard_num > 1) ? shard_num : 2;
        int64_t seconds = (upper-lower)*365*24*3600 / need;
        out<<"\"datetime-lower\"=\"";
        out << lower << "-01-01 00:00:00";
        out<<"\",\n";

        out<<"\"datetime-upper\"=\"";
        out << upper << "-01-01 00:00:00";
        out<<"\",";

        out<<"\"sharding-seconds\"=\"";
        out << seconds;
        out<<"\"";
    }
    
    out<<"))\n";
    out<<");";
}

void create_binding_table_rule::out(std::ostream &out){
    out<<"CREATE SHARDING TABLE REFERENCE RULE ref_0 (";
    int victim_nums = victim_names.size();
    int i = 0;
    for(auto& s:victim_names){
        out<<s;
        if(i != victim_nums-1){
            out<<",";
        }
        i++;
    }
    out<<");";
}

std::pair<std::string, std::string> split_at_first_newline(const std::string& s) {
    size_t pos = s.find('\n');
    if (pos == std::string::npos) {
        return {s, ""};
    }
    return {s.substr(0, pos), s.substr(pos + 1)};
}

//this function is used to create a dds, and then execute them.
string ss_distributor::distribute_one(shared_ptr<prod> gen, int* affect_num, int i){
    auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
    if(create_table == nullptr){
        throw runtime_error("not create_table_stmt");
    }
    int groupid = i % total_groups;
    int id_in_group = i / total_groups;
    shared_ptr<create_dds> dds;
    if(id_in_group == 0){
        //just local table
        //cout<< "stored in "<<storage_units[0]<<endl;
        auto d = make_shared<create_single_table_rule>(create_table->created_table,storage_units[0], gen.get());
        // ostringstream ss;
        // d->out2(ss);
        // auto sql = ss.str();
        // ctx -> master_dut -> test(sql);
        dds = d;
    }
    else if(id_in_group == 1){
        // reference table
        dds = make_shared<create_broadcast_table_rule>(create_table->created_table, gen.get());
    }else if(id_in_group == 2){
        //coloacted table, create_distributed_table(shared_ptr<table> victim_table, string colocate, shared_ptr<column> dkey = nullptr,  prod *parent = nullptr);
        if(groupid != 0){
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");

            dds = make_shared<create_sharding_table_rule>(
                create_table->created_table,
                this -> used_for_binding ->storage_units,
                this -> used_for_binding ->strategy,
                this -> used_for_binding ->dkey,
                this -> used_for_binding ->shard_num,
                gen.get()
            );
            this ->binding_tables.push_back(create_table->created_table->name);
        }else{
            vector<sqltype *> enable_type;
            enable_type.push_back(gen->scope->schema->inttype);
            enable_type.push_back(gen->scope->schema->texttype);
            enable_type.push_back(gen->scope->schema->realtype);
            // enable_type.push_back(gen->scope->schema->datetype);
            auto type = random_pick<>(enable_type);

            colocate_column = make_shared<column>(COLOCATE_NAME, type);
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");
            
            this -> used_for_binding = make_shared<create_sharding_table_rule>(create_table->created_table, storage_units, "", colocate_column, -1, gen.get());
            dds = this -> used_for_binding;
            this ->binding_tables.push_back(create_table->created_table->name);
        }

    }else if(id_in_group == 3){
        if(groupid == 0){
            //run binding tables
            auto colocated_dds = make_shared<create_binding_table_rule>(this -> binding_tables);
            ostringstream s2;
            colocated_dds->out(s2);
            auto sql = s2.str();
            ctx -> master_dut -> test(sql);
        }
        int unit_nums = storage_units.size();
        int i = dx(unit_nums -1);// dont store in units[0]
        dds = make_shared<create_single_table_rule>(create_table->created_table,storage_units[i], gen.get());
    }
    else{
        //distributed table
        // dds = make_shared<create_distributed_table>(create_table->created_table, "" ,nullptr, gen.get());
        dds = make_shared<create_sharding_table_rule>(create_table->created_table, storage_units, "", nullptr, -1, gen.get());
    }

    // create rule before create table
    ostringstream s2;
    dds->out(s2);
    auto sql = s2.str();
    ctx -> master_dut -> test(sql);

    bool exist = false;
    bool should_delete = true;
    string create_table_sql;
    while(!exist){
        try{
            ostringstream s;
            gen->out(s);
            create_table_sql = s.str()+";";
            //split
            auto [first_line, rest] = split_at_first_newline(create_table_sql);
            if(should_delete)
                ctx -> master_dut -> test(first_line);
            should_delete = false;

            if(!rest.empty()){
                ctx -> master_dut -> test(rest);
            }
            string table_name = create_table->created_table->ident();
            for(int x= 0;x < 5;x++){
                try{
                    sleep(1);
                    string test_exist = "select count(*) from " + table_name + ";";
                    ctx -> master_dut -> test(test_exist);
                    exist = true;
                    break;
                }catch(exception& e){
                    continue;
                }
            }
        }catch(exception& e){
            string err = e.what();
            if(err.find("already exists") != string::npos){
                break;
            }
            else continue;
        }

    }
    
    return create_table_sql + sql;
}

void ss_distributor::clear_record_file(void){
    //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
    std::ofstream file(string(SS_SAVING_DIR) + string(SS_DIST_RECORD_FILE), std::ios::trunc);
    file.close();
}

// bool ss_distsql_action::run(Context& ctx){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     try{
//         context.master_dut -> test(sql);
//         impact(context);
//     }catch(exception& e){
//         // string s = "#"+sql+" "+context.master_dut -> ip+" "+
//         // to_string(context.master_dut -> port)+" "
//         // +context.master_dut ->db;
//         // log(s, context.logfile);
//         throw;
//     }
//     //log without ip, port, db
//     log(sql, context.logfile);
//     return true;
// }


// register_storage_unit::register_storage_unit(shared_ptr<ss_worker_connection> c){
//     sql = "REGISTER STORAGE UNIT ds_" + to_string(c -> id) + " ("
//     + "HOST=\"" + c -> test_ip + "\","
//     + "PORT=" + to_string(c -> test_port) + ","
//     + "DB=\"" + c -> test_db + "\","
//     + "USER=\"" + c -> user + "\","
//     + "PASSWORD=\"" + c -> password + "\");";
//     db = c -> test_db;
//     ip = c -> test_ip;
//     port = c -> test_port;
//     user = c -> user;
//     password = c -> password;
// }

// void register_storage_unit::impact(ss_context& context){
//     for(auto& c : context.workers){
//         if(c -> test_ip == ip && c -> test_port == port){
//             c -> connected_to_cluster = true;
//             break;
//         }
//     }
// }

// void register_storage_unit::random_fill(Context& ctx){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     vector<shared_ptr<ss_worker_connection>> unconnected_workers;
//     shared_ptr<ss_worker_connection> victim;
//     for(auto& c : context.workers){
//         if(! c -> connected_to_cluster){
//             unconnected_workers.push_back(c);
//         }
//     }
//     if(unconnected_workers.size() == 0){
//         //expected to be worng
//         victim = random_pick(context.workers);
//     }else{
//         victim = random_pick(unconnected_workers);
//     }
//     db = victim -> test_db;
//     ip = victim -> test_ip;
//     port = victim -> test_port;
//     user = victim -> user;
//     password = victim -> password;
//     sql = "REGISTER STORAGE UNIT ds_" + to_string(victim -> id) + " ("
//     + "HOST=\"" + ip + "\","
//     + "PORT=" + to_string(port) + ","
//     + "DB=\"" + db + "\","
//     + "USER=\"" + user + "\","
//     + "PASSWORD=\"" + password + "\");";
// }

// void unregister_storage_unit::random_fill(Context& ctx){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     vector<shared_ptr<ss_worker_connection>> connected_workers;
//     shared_ptr<ss_worker_connection> victim;
//     for(auto& c : context.workers){
//         if(c -> connected_to_cluster){
//             connected_workers.push_back(c);
//         }
//     }
//     if(connected_workers.size() == 0){
//         //expected to be worng
//         victim = random_pick(context.workers);
//     }else{
//         victim = random_pick(connected_workers);
//     }
//     ip = victim -> test_ip;
//     port = victim -> test_port;
//     sql = "UNREGISTER STORAGE UNIT ds_" + to_string(victim -> id) + ";";
// }

// void unregister_storage_unit::impact(ss_context& context){
//     for(auto& c : context.workers){
//         if(c -> test_ip == ip && c -> test_port == port){
//             c -> connected_to_cluster = false;
//             break;
//         }
//     }
// }

// bool ss_distribute_action::run(Context& ctx, int* affect_num){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     try{
//         ostringstream s;
//         create_table->out(s);
//         //separate s by ;
//         vector<string> sqls;
//         auto create_table_str = s.str();
//         stringstream ss(create_table_str);
//         string item;
//         while(getline(ss, item, ';')){
//             sqls.push_back(item);
//         }
//         context.master_dut -> test(sql, NULL, affect_num);
//         context.master_dut -> test(sqls[0] + ';', NULL, affect_num);
//         context.master_dut -> test(sqls[1] + ';', NULL, affect_num);
//         log(sql, context.logfile);
//         log(create_table_str+";", context.logfile);
//     }catch(exception& e){
//         // do nothing
//         throw;
//     }
    
//     return true;
// }

// void autoTableRuleDefinition(string& sql, ss_context& context,shared_ptr<table>& created_table){
//     sql +=  "STORAGE_UNITS(";
//     vector<int> ids;
//     vector<int> v_ids;
//     for(auto& c : context.workers){
//         if(c -> connected_to_cluster){
//             ids.push_back(c -> id);
//             if(d6() == 1) v_ids.push_back(c -> id);
//         }
//     }
//     if(v_ids.size() == 0){
//         //expected to be wrong
//         v_ids = ids;
//     }
//     for(auto it = v_ids.begin(); it != v_ids.end(); ++it){
//         sql += "ds_" + to_string(*it);
//         if(std::next(it) != v_ids.end()) {
//             sql += ", ";
//         }
//     }
//     sql += "), SHARDING_COLUMN=";
//     auto columns_in_table = created_table -> columns();
//     column* dist_column = &random_pick(columns_in_table);
//     sql += dist_column -> name;
//     sql += ", TYPE(NAME=";
//     if(d6()<3 || dist_column -> type -> name == "text"){
//         int sharding_count = dx(20);
//         sql +="\"hash_mod\",PROPERTIES(\"sharding-count\"=\""+ to_string(sharding_count) +"\")";
//     }else{
//         int sharding_count = dx(20);
//         sql +="\"mod\",PROPERTIES(\"sharding-count\"=\""+ to_string(sharding_count) +"\")";
//     }
//     //MATCH "TYPE"
//     sql += ")";
// }

// void create_sharding_table_rule::random_fill(Context& ctx){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     create_table = context.temp_table;
//     sql = "CREATE SHARDING TABLE RULE ";
//     if(d6()<3) sql += "IF NOT EXISTS ";
//     sql += context.temp_table -> created_table -> name + " (";
//     autoTableRuleDefinition(sql,context,context.temp_table -> created_table);
//     // if(d6()<5){
//         sql += ", KEY_GENERATE_STRATEGY(COLUMN=";
//         vector<string> key_column_names = {"pkey","vkey"};
//         string key_column_name = random_pick(key_column_names);
//         vector<string> key_generate_strategy_names = {"SNOWFLAKE","UUID"};
//         string key_generate_strategy_name = random_pick(key_generate_strategy_names);
//         sql += key_column_name + ",TYPE(NAME=\""+ key_generate_strategy_name +"\"))";
//     // }
//     if(d6()<5){
//         sql += ", AUDIT_STRATEGY (TYPE(NAME=\"DML_SHARDING_CONDITIONS\"),ALLOW_HINT_DISABLE=true)";
//     }
//     sql += ");";
// }

// void alter_sharding_table_rule::random_fill(Context& ctx){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     sql = "ALTER SHARDING TABLE RULE ";
//     //choose a table
//     context.get_schema() -> refresh();
//     vector<shared_ptr<table>> tables;
//     for(auto& t : context.get_schema() -> tables){
//         if(context.get_schema() -> sharding_tables.count(t.name) > 0){
//             tables.push_back(make_shared<table>(t));
//         }
//     }
//     shared_ptr<table> t = random_pick(tables);
//     sql += t -> name + " (";
//     autoTableRuleDefinition(sql,context,t);
//     //if(d6()<5){
//         sql += ", KEY_GENERATE_STRATEGY(COLUMN=";
//         vector<string> key_column_names = {"pkey","vkey"};
//         string key_column_name = random_pick(key_column_names);
//         vector<string> key_generate_strategy_names = {"SNOWFLAKE","UUID"};
//         string key_generate_strategy_name = random_pick(key_generate_strategy_names);
//         sql += key_column_name + ",TYPE(NAME=\""+ key_generate_strategy_name +"\"))";
//     //}
//     if(d6()<5){
//         sql += ", AUDIT_STRATEGY (TYPE(NAME=\"DML_SHARDING_CONDITIONS\"),ALLOW_HINT_DISABLE=true)";
//     }
//     sql += ");";
// }

// void create_broadcast_table_rule::random_fill(Context& ctx){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     create_table = context.temp_table;
//     sql = "CREATE BROADCAST TABLE RULE ";
//     if(d6()<3) sql += "IF NOT EXISTS ";
//     sql += context.temp_table -> created_table -> name + ";";
// }

// void load_single_table::random_fill(Context& ctx){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     create_table = context.temp_table;
//     sql = "LOAD SINGLE TABLE ";
//     vector<int> ids;
//     for(auto& c : context.workers){
//         if(c -> connected_to_cluster){
//             ids.push_back(c -> id);
//         }
//     }
//     vicitm_id = random_pick(ids);
//     sql += "ds_" + to_string(vicitm_id) + ".";
//     if(context.single_dbms_name == "postgres"){
//         sql += "public.";
//     }
//     sql += context.temp_table -> created_table -> name + ";";
// }

// bool load_single_table::run(Context& ctx, int* affect_num){
//     auto& context = dynamic_cast<ss_context&>(ctx);
//     try{
//         ostringstream s;
//         create_table->out(s);
//         auto create_table_str = s.str();
//         for(auto& c : context.workers){
//             if(c -> id == vicitm_id){
//                 c -> dut -> test(create_table_str+";", NULL, affect_num);
//             }
//         }
//         context.master_dut -> test(sql); 
//         log(sql, context.logfile);
//         log(create_table_str+";", context.logfile);
//     }catch(exception& e){
//         // do nothing
//         throw;
//     }
//     return true;
// }
