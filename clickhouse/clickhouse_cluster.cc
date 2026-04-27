#include "clickhouse_cluster.hh"

clickhouse_cluster_fuzzer::clickhouse_cluster_fuzzer(shared_ptr<clickhouse_context> ctx)
: cluster_fuzzer(ctx, CLICKHOUSE_SAVING_DIR + string(CLICKHOUSE_CLUSTER_ACTIONS))
{
    base_context = ctx;
    auto context = dynamic_pointer_cast<clickhouse_context>(base_context);
    context -> logfile = CLICKHOUSE_SAVING_DIR + string(CLICKHOUSE_CLUSTER_ACTIONS);
    // context -> master_dut = make_shared<tidb_dut>(db, ip, port);
}

void clickhouse_cluster_fuzzer::reset_init(){
    auto context = dynamic_pointer_cast<clickhouse_context>(base_context);
    context -> master_dut -> reset();
}

on_cluster::on_cluster(shared_ptr<table> victim_table,prod *parent)
:create_dds(victim_table,parent){
    // if(d6()<3){
    //     cluster = "";
    // }else 
    if(d6()<4){
        cluster = get_cluster_name();
    }
}
void on_cluster::out(std::ostream &out){
    if(cluster == "") return;
    out<<" on cluster "<<cluster<<endl;
}

table_partitions::table_partitions(shared_ptr<table> victim_table, prod *parent, string pkey, string ex)
:create_dds(victim_table,parent){
    if(pkey != ""){
        for(auto& c:victim_table->columns()){
            if(c.name == pkey){
                this->partition_key = make_shared<column>(c);
            }
        }
    }
    if(this->partition_key == nullptr){
        this->partition_key = make_shared<column>(random_pick(victim_table->columns()));
    }
    int i = d12();
    if(ex != ""){
        this->expr = ex;
    }
    else if(i == 1 && this->partition_key->type != parent->scope->schema->realtype){
        this->expr = this->partition_key->name;
    }
    else if(i <= 4){
        this->expr = "cityHash64("+this->partition_key->name+")";
    }
    else if(this->partition_key->type == parent->scope->schema->inttype && i <= 8){
        this->expr = this->partition_key->name+" % "+to_string(d12());
    }
    else if(this->partition_key->type == parent->scope->schema->realtype&& i <= 8){
        this->expr = "cityHash64(toString("+this->partition_key->name+")) % "+to_string(d12());
    }
    else if(this->partition_key->type == parent->scope->schema->texttype && i <= 8){
        this->expr = "cityHash64("+this->partition_key->name+")";
    }
    else if(this->partition_key->type == parent->scope->schema->datetype && i <= 8){
        string func;
        int j = d6();
        if(j == 1){
            func = "toDayOfMonth";
        }
        else if (j == 2){
            func = "toDayOfWeek";
        }
        else if (j == 3){
            func = "toDayOfYear";
        }
        else if (j == 4){
            func = "toDaysSinceYearZero";
        }
        else if (j == 5){
            func = "toHour";
        }
        else{
            func = "toUnixTimestamp";
        }
        j = d12();
        if(j < 4)
            this->expr = func +"("+this->partition_key->name+")";
        else if(j < 7)
            this->expr = func +"("+this->partition_key->name+") % "+to_string(d12());
        else if(j < 10)
            this->expr = "cityHash64("+func +"("+this->partition_key->name+"))";
        else
            this->expr = "cityHash64("+func +"("+this->partition_key->name+")) % "+to_string(d12());
    }else{
        this->expr = "cityHash64("+this->partition_key->name+") % "+to_string(d12());
    }
}

void table_partitions::out(std::ostream &out){
    out << " PARTITION BY "<< this->expr;
}

void table_partitions::write_dds(string name){
    string path = CLICKHOUSE_SAVING_DIR + string(CLICKHOUSE_DIST_DKEY_FILE);
    ofstream ofile(path, ios::app);
    ofile << name <<" " << this->partition_key->name << endl;
    ofile.close();
}

void clickhouse_origin_table::out(std::ostream &out){
    if(origin_create_stmt==nullptr)
        throw runtime_error("origin_create_stmt is nullptr in clickhouse_origin_table");
    out << "DROP TABLE IF EXISTS " << mask_name << origin_create_stmt->created_table->name << ";\n";
    out << "create table ";
    out << mask_name << origin_create_stmt->created_table->name;
    if(cluster != nullptr){
        out << *cluster;
    }
    out << " ( ";
    indent(out);

    auto columns_in_table = origin_create_stmt->created_table->columns();
    int column_num = columns_in_table.size();
    for (int i = 0; i < column_num; i++) {
        out << columns_in_table[i].name << " ";
        out << columns_in_table[i].type->name << " ";
        out << origin_create_stmt->constraints[i];
        if (i != column_num - 1)
            out << ",";
        indent(out);
    }

    // if (has_primary_key) {
    //     out << ",";
    //     indent(out);
    //     out << "primary key(" << primary_key_str << ")";
    // }

    // if (has_check) {
    //     out << ",";
    //     indent(out);
    //     out << "check(" << *check_expr << ")";
    // }
    indent(out);
    out << ")";

    if(partitions != nullptr){
        out << *partitions;
    }
    out << ";"<<endl;
}

distributed_table::distributed_table(shared_ptr<table> victim_table, shared_ptr<clickhouse_origin_table> gen,string db,
         prod *parent,string pkey , string ex):create_dds(victim_table,parent){
    if(pkey != "" && ex == ""){
        for(auto& c:victim_table->columns()){
            if(c.name == pkey){
                this->shard_key = make_shared<column>(c);
            }
        }
    }
    if(this->shard_key == nullptr && ex == ""){
        this->shard_key = make_shared<column>(random_pick(victim_table->columns()));
    }
    if(gen == nullptr)
        throw runtime_error("clickhouse_origin_table is nullptr in clickhouse_distributed_table");
    origin = gen;
    this -> db = db;
    int i = d12();
    if(ex != ""){
        this->expr = ex;
    }
    else if(i <= 4){
        this->expr = "cityHash64("+this->shard_key->name+")";
    }
    else if(this->shard_key->type == parent->scope->schema->inttype && i <= 8){
        this->expr = this->shard_key->name+" % "+to_string(d12());
    }
    else if(this->shard_key->type == parent->scope->schema->realtype&& i <= 8){
        this->expr = "cityHash64(toString("+this->shard_key->name+")) % "+to_string(d12());
    }
    else if(this->shard_key->type == parent->scope->schema->texttype && i <= 8){
        this->expr = "cityHash64("+this->shard_key->name+")";
    }
    else if(this->shard_key->type == parent->scope->schema->datetype && i <= 8){
        string func;
        int j = d6();
        if(j == 1){
            func = "toDayOfMonth";
        }
        else if (j == 2){
            func = "toDayOfWeek";
        }
        else if (j == 3){
            func = "toDayOfYear";
        }
        else if (j == 4){
            func = "toDaysSinceYearZero";
        }
        else if (j == 5){
            func = "toHour";
        }
        else{
            func = "toUnixTimestamp";
        }
        j = d12();
        if(j < 4)
            this->expr = func +"("+this->shard_key->name+")";
        else if(j < 7)
            this->expr = func +"("+this->shard_key->name+") % "+to_string(d12());
        else if(j < 10)
            this->expr = "cityHash64("+func +"("+this->shard_key->name+"))";
        else
            this->expr = "cityHash64("+func +"("+this->shard_key->name+")) % "+to_string(d12());
    }else{
        this->expr = "cityHash64("+this->shard_key->name+") % "+to_string(d12());
    }
    table_name = origin ->origin_create_stmt -> created_table->name;
    origin -> mask_name = "single_";
}

void distributed_table::out(std::ostream &out){
    out<<"CREATE TABLE " << table_name ;
    if(origin -> cluster != nullptr){
        out<<*(origin -> cluster);
    }
    out << " as " << origin -> mask_name << origin ->origin_create_stmt -> created_table->name<<endl;
    out << "ENGINE = Distributed('";
    out << (origin -> cluster ->cluster) <<"','";
    out << db <<"','";
    out << origin -> mask_name << origin ->origin_create_stmt -> created_table->name <<"', ";
    out << expr <<");"<<endl;
}

void distributed_table::write_dds(string name,string dkey){
    string path = CLICKHOUSE_SAVING_DIR + string(CLICKHOUSE_DIST_DKEY_FILE);
    ofstream ofile(path, ios::app);
    string dkey_out = dkey;
    if(dkey_out == ""){
        dkey_out = this->shard_key->name;
    }
    ofile << name <<" " << dkey_out << endl;
    ofile.close();
}

string clickhouse_distributor::distribute_one(shared_ptr<prod> gen, int* affect_num, int i){
    auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
    if(create_table == nullptr){
        throw runtime_error("not create_table_stmt");
    }
    int groupid = i % total_groups;
    int id_in_group = i / total_groups;
    shared_ptr<create_dds> dds = nullptr;
    shared_ptr<clickhouse_origin_table> origin = nullptr;

    if(id_in_group == 0){
        ostringstream s;
        gen->out(s);
        auto sql = s.str() + ";";
        ctx -> master_dut -> test(sql);

        ofstream ofile(string(CLICKHOUSE_SAVING_DIR)+CLICKHOUSE_DIST_RECORD_FILE, ios::app);
        ofile << sql << endl;
        ofile.close();

        return sql;
    }else if(id_in_group == 1){
        auto partition = make_shared<table_partitions>(create_table->created_table,gen.get());
        origin = make_shared<clickhouse_origin_table>(create_table->created_table,create_table,nullptr,partition,gen.get());
    }else if(id_in_group == 2){
        if(groupid != 0){
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");

            auto partition = make_shared<table_partitions>(create_table->created_table,gen.get());
            if(d6() == 1) partition == nullptr;
            origin = make_shared<clickhouse_origin_table>(create_table->created_table,create_table,
                cluster_used_for_binding,partition,gen.get());
            auto d  = make_shared<distributed_table>(create_table->created_table,origin,ctx -> info.test_db ,
                    gen.get(),COLOCATE_NAME,shard_expr_for_binding);
            dds = d;
            d->write_dds(create_table->created_table->ident(),COLOCATE_NAME);
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

            cluster_used_for_binding = make_shared<on_cluster>(create_table->created_table,get_cluster_name(),gen.get());
            auto partition = make_shared<table_partitions>(create_table->created_table,gen.get());
            if(d6() == 1) partition == nullptr;
            origin = make_shared<clickhouse_origin_table>(create_table->created_table,create_table,
                cluster_used_for_binding,partition,gen.get());
            auto d = make_shared<distributed_table>(create_table->created_table,origin,ctx -> info.test_db ,gen.get(),COLOCATE_NAME,"");
            shard_expr_for_binding = d -> expr;
            dds = d;
            d->write_dds(create_table->created_table->ident());
        }
    }else{
        shared_ptr<on_cluster> cluster = make_shared<on_cluster>(create_table->created_table,get_cluster_name(),gen.get());
        shared_ptr<table_partitions> partitions = nullptr;
        if(d6() != 1){
            partitions = make_shared<table_partitions>(create_table->created_table,gen.get());
        }
        origin = make_shared<clickhouse_origin_table>(create_table->created_table,create_table,cluster,partitions,gen.get());
        auto d = make_shared<distributed_table>(create_table->created_table,origin,ctx -> info.test_db ,gen.get());
        d->write_dds(create_table->created_table->ident());
        dds=d;
    }
    
    if(origin){
        ostringstream s;
        origin->out(s);
        auto sql1 = s.str();
        ctx -> master_dut -> test(sql1);

        ofstream ofile(string(CLICKHOUSE_SAVING_DIR)+CLICKHOUSE_DIST_RECORD_FILE, ios::app);
        ofile << sql1 << endl;
        ofile.close();
    }

    if(dds){
        ostringstream s;
        dds->out(s);
        auto sql1 = s.str();
        ctx -> master_dut -> test(sql1);

        ofstream ofile(string(CLICKHOUSE_SAVING_DIR)+CLICKHOUSE_DIST_RECORD_FILE, ios::app);
        ofile << sql1 << endl;
        ofile.close();
    }

    ostringstream ss;
    gen->out(ss);
    auto sql = ss.str() + ";";
    // ctx -> master_dut -> test(sql);
    return sql;
}

void clickhouse_distributor::clear_record_file(void){
    //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
    std::ofstream file(string(CLICKHOUSE_SAVING_DIR) + string(CLICKHOUSE_DIST_RECORD_FILE), std::ios::trunc);
    file.close();
    std::ofstream file1(string(CLICKHOUSE_SAVING_DIR) + string(CLICKHOUSE_DIST_DKEY_FILE), std::ios::trunc);
    file1.close();
}