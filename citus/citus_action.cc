#include "citus_action.hh"
#include "citus.hh"


bool citus_create_dds::run(Context& ctx){
    stringstream ss;
    this->out(ss);
    string sql = ss.str();
    auto& context = dynamic_cast<citus_context&>(ctx);
    try{
        context.get_dut() -> test(sql);
    }catch(exception& e){
        string s = "#"+sql+" "+context.get_dut() -> postgres_dut -> test_ip+" "+
        to_string(context.get_dut() -> postgres_dut -> test_port)+" "
        +context.get_dut() -> postgres_dut -> test_db;
        log(s, context.logfile);
        throw;
    }
    //log with ip, port, db
    log(sql+" "+context.get_dut() -> postgres_dut -> test_ip+" "+
    to_string(context.get_dut() -> postgres_dut -> test_port)+" "
    +context.get_dut() -> postgres_dut -> test_db,
    context.logfile);
    return true;
}

create_distributed_table::create_distributed_table(shared_ptr<table> victim_table, shared_ptr<column> dkey, int shard_num, prod *parent): 
    citus_create_dds(victim_table, parent){
    //cout<<"a"<<endl;
    use_colocate = false;
    schema_citus* db_schema = dynamic_cast<schema_citus*>(parent->scope->schema);
    if(victim_table == nullptr){
        vector<table*> undistributed_tables;
        for(table& c_t : db_schema -> tables){
            bool is_distributed = db_schema->distributed_tables.count(c_t.name) > 0;
            bool is_reference = db_schema->reference_tables.count(c_t.name) > 0;
            
            if(!is_distributed && !is_reference) {
                undistributed_tables.push_back(&c_t);
            }
        }
        if(undistributed_tables.empty()){
            throw runtime_error("No candidates available");
        }
        victim = make_shared<table>(*random_pick(undistributed_tables));
    }else{
        victim = victim_table;
    }
    if(dkey == nullptr){
        // for(table& c_t : db_schema -> tables){
        //     if(c_t.name == victim -> name){
        //         if(c_t.constraints.empty()){
        //             int col_index = d100() % victim -> columns().size();
        //             dkey = make_shared<column>(victim -> columns()[col_index]);
        //         }
        //         else{
        //             string col_name = random_pick(c_t.constraints);
        //             for(column& c_col : victim -> columns()){
        //                 if(c_col.name == col_name){
        //                     dkey = make_shared<column>(c_col);
        //                     break;
        //                 }
        //             }
        //         }
        //         break;
        //     }
        // }
        column col = random_pick(victim -> columns());
        this -> dkey = make_shared<column>(col);
    }else{
        this -> dkey = dkey;
    }

    if(shard_num == -1){
        if(d6() < 2){
            shard_num = dx(4) + 1;
        }
        else if(d6() < 4){
            shard_num = dx(20) + 5;
        }
    }
}

create_distributed_table::create_distributed_table(shared_ptr<table> victim_table, string colocate, shared_ptr<column> dkey, prod *parent): 
    citus_create_dds(victim_table, parent){

    //cout<<"b"<<endl;
    schema_citus* db_schema = dynamic_cast<schema_citus*>(parent->scope->schema);
    shared_ptr<citus_table> colocate_table;
    if(colocate == ""){
        //db_schema->refresh();
        if(db_schema -> distributed_tables.empty()){
            this -> use_colocate = false;
            column col = random_pick(victim -> columns());
            this -> dkey = make_shared<column>(col);
            shard_num = d6() < 2 ? dx(20) + 5 : dx(4) + 1;
            return;
        }
        table x = table(colocate, "main", true, true);
        int groupid = x.get_id_from_name() % total_groups;
        vector<shared_ptr<citus_table>> candidates;
        for(auto& t : db_schema -> distributed_tables){
            int t_groupid = t.second -> get_id_from_name() % total_groups;
            int t_id_in_group = t.second -> get_id_from_name() / total_groups;
            if(t_groupid != groupid && t_id_in_group != 2){
                candidates.push_back(t.second);
            }
        }
        if(candidates.empty()){
            this -> use_colocate = false;
            column col = random_pick(victim -> columns());
            this -> dkey = make_shared<column>(col);
            shard_num = d6() < 2 ? dx(20) + 5 : dx(4) + 1;
            return;
        }
        colocate_table = random_pick(candidates);
        this -> colocate = colocate_table -> name;
    }else{
        this -> colocate = colocate;
    }

    if(dkey == nullptr){
        // for(table& c_t : db_schema -> tables){
        //     if(c_t.name == victim -> name){
        //         if(c_t.constraints.empty()){
        //             int col_index = d100() % victim -> columns().size();
        //             dkey = make_shared<column>(victim -> columns()[col_index]);
        //         }
        //         else{
        //             string col_name = random_pick(c_t.constraints);
        //             for(column& c_col : victim -> columns()){
        //                 if(c_col.name == col_name){
        //                     dkey = make_shared<column>(c_col);
        //                     break;
        //                 }
        //             }
        //         }
        //         break;
        //     }
        // }
        column* col = colocate_table -> distribution_column;
        //cout << colocate_table->name<< " " << col -> name << " " << col -> type -> name << endl;
        vector<shared_ptr<column>> candidates;
        for(column& c_col : victim -> columns()){
            if(c_col.type == col -> type){
                candidates.push_back(make_shared<column>(c_col));
                //cout<<"candidates: " << c_col.name << " " << c_col.type -> name << endl;
            }
        }
        if(candidates.empty()){
            this -> use_colocate = false;
            column col = random_pick(victim -> columns());
            this -> dkey = make_shared<column>(col);
            shard_num = d6() < 2 ? dx(20) + 5 : dx(4) + 1;
            return;
        }
        this -> dkey = random_pick(candidates);
    }else{
        this -> dkey = dkey;
    }
    this -> use_colocate = true;
}

void create_distributed_table::out(std::ostream &out){
    out << "SELECT create_distributed_table('" << victim -> name;
    out << "','" << dkey -> name << "'";
    if(use_colocate){
        out << ", colocate_with:='" << colocate << "');";
    }
    else if(shard_num != -1){
        out<<", shard_count:="<<shard_num<<");";
    }
    else{
        out<<");";
    }
}

create_reference_table::create_reference_table(shared_ptr<table> victim_table, prod *parent)
        : citus_create_dds(victim_table, parent){
    if(victim_table != nullptr){
        victim_table_name = victim_table -> name;
        return;
    }
    throw runtime_error("we expect a victim table for create_reference_table()");
}

void create_reference_table::out(std::ostream &out){
    out << "SELECT create_reference_table('" << victim_table_name << "');";
}

//this function is used to create a dds for a create_table_stmt, and then execute them.
string citus_distributor::distribute_one(shared_ptr<prod> gen, int* affect_num, int i){
    auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
    if(create_table == nullptr){
        throw runtime_error("not create_table_stmt");
    }
    int groupid = i % total_groups;
    int id_in_group = i / total_groups;
    shared_ptr<create_dds> dds;
    if(id_in_group == 0){
        //just local table
        ostringstream s;
        gen->out(s);
        auto create_table_sql = s.str()+";";
        ctx -> master_dut -> test(create_table_sql);
        return create_table_sql;
    }
    else if(id_in_group == 1){
        // reference table
        dds = make_shared<create_reference_table>(create_table->created_table, gen.get());
    }else if(id_in_group == 2){
        //coloacted table, create_distributed_table(shared_ptr<table> victim_table, string colocate, shared_ptr<column> dkey = nullptr,  prod *parent = nullptr);
        if(groupid != 0){
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");
            dds = make_shared<create_distributed_table>(create_table->created_table,
                "t"+to_string(i - groupid), colocate_column, gen.get());
        }else{
            vector<sqltype *> enable_type;
            enable_type.push_back(gen->scope->schema->inttype);
            enable_type.push_back(gen->scope->schema->texttype);
            enable_type.push_back(gen->scope->schema->realtype);
            enable_type.push_back(gen->scope->schema->datetype);
            auto type = random_pick<>(enable_type);

            colocate_column = make_shared<column>(COLOCATE_NAME, type);
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");
            dds = make_shared<create_distributed_table>(create_table->created_table, colocate_column, -1, gen.get());
        }

    }
    // else if(d6() < 4){
    //     // random colocate
    //     dds = make_shared<create_distributed_table>(create_table->created_table, "" ,nullptr, gen.get());
    // }
    else{
        //distributed table
        dds = make_shared<create_distributed_table>(create_table->created_table, "" ,nullptr, gen.get());
        //dds = make_shared<create_distributed_table>(create_table->created_table, nullptr, -1, gen.get());
    }
    ostringstream s;
    gen->out(s);
    auto create_table_sql = s.str()+";";
    ctx -> master_dut -> test(create_table_sql);
    
    ostringstream s2;
    dds->out(s2);
    auto sql = s2.str();
    ctx -> master_dut -> test(sql);

    ofstream ofile(CITUS_SAVING_DIR + string(CITUS_DIST_RECORD_FILE), ios::app);
    ofile << sql << endl;
    ofile.close();

    return create_table_sql + sql;
}

void citus_distributor::clear_record_file(void){
    //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
    std::ofstream file(string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE), std::ios::trunc);
    file.close();
}