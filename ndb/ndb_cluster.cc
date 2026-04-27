#include "ndb_cluster.hh"

ndb_cluster_fuzzer::ndb_cluster_fuzzer(shared_ptr<ndb_context> ctx)
: cluster_fuzzer(ctx, NDB_SAVED + string(NDB_CLUSTER_ACTIONS)){
    base_context = ctx;
    auto context = dynamic_pointer_cast<ndb_context>(base_context);
    context -> logfile = NDB_SAVED + string(NDB_CLUSTER_ACTIONS);
}

void ndb_cluster_fuzzer::reset_init(){
    auto context = dynamic_pointer_cast<ndb_context>(base_context);
    context -> master_dut ->reset();
}


// ndb_distributor::ndb_distributor(shared_ptr<Context> context){}

void ndb_create_table::out(std::ostream &out){
    out << "DROP TABLE IF EXISTS " << origin_stmt->created_table->name << ";\n";
    out << "create table ";
    out << origin_stmt->created_table->name << " ( ";
    indent(out);

    auto columns_in_table = origin_stmt->created_table->columns();
    int column_num = columns_in_table.size();
    for (int i = 0; i < column_num; i++) {
        out << columns_in_table[i].name << " ";
        out << columns_in_table[i].type->name << " ";
        out << origin_stmt->constraints[i];
        if (i != column_num - 1)
            out << ",";
        indent(out);
    }
    indent(out);
    out << ")";
    if(engine != ""){
        out<<" ENGINE="<<engine<<endl;
    }
    if(fully_replicated){
        out<<" COMMENT='NDB_TABLE=FULLY_REPLICATED=1'";
    }
    if(partition_key_index!=-1 && pcount >= 1){
        out<<" PARTITION BY KEY("<<columns_in_table[partition_key_index].name<<") PARTITIONS "<<pcount<<endl;
    }
    out<<";"<<endl;
}

void ndb_create_table::assign_a_partition_key(string name){
    auto columns_in_table = origin_stmt->created_table->columns();
    int column_num = columns_in_table.size();
    if(name != ""){
        for (int i = 0; i < column_num; i++) {
            if(columns_in_table[i].name ==name){
                partition_key_index = i;
                break;
            }
        }
    }
    if(partition_key_index == -1){
        partition_key_index = dx(column_num) - 1;
    }
    origin_stmt->constraints[partition_key_index] = "PRIMARY KEY";
}

void ndb_distributor::clear_record_file(){
    std::ofstream file1(string(NDB_SAVED) + string(NDB_CLUSTER_ACTIONS), std::ios::trunc);
    file1.close();
}

string ndb_distributor:: distribute_one(shared_ptr<prod> gen, int* affect_num, int i){
    auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
    if(create_table == nullptr){
        throw runtime_error("not create_table_stmt");
    }
    int groupid = i % total_groups;
    int id_in_group = i / total_groups;

    shared_ptr<ndb_create_table> new_table=make_shared<ndb_create_table>(create_table, gen.get());
    new_table -> engine = "NDBCLUSTER";

    if(id_in_group == 0){
        new_table->engine = "InnoDB";
    }
    else if(id_in_group == 1){
        // if(d6()<3){
        //     new_table->assign_a_partition_key();
        //     new_table->pcount = d12();
        // }
        new_table->fully_replicated = true;
    }
    else if(id_in_group == 2){
        if(groupid != 0){
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("PRIMARY KEY");
            new_table->assign_a_partition_key(COLOCATE_NAME);
            new_table->pcount = colocated_pcount;
        }else{
            vector<sqltype *> enable_type;
            enable_type.push_back(gen->scope->schema->inttype);
            enable_type.push_back(gen->scope->schema->texttype);
            enable_type.push_back(gen->scope->schema->realtype);
            enable_type.push_back(gen->scope->schema->datetype);
            auto type = random_pick<>(enable_type);

            colocate_column = make_shared<column>(COLOCATE_NAME, type);
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("PRIMARY KEY");
            colocated_pcount = d6();
            new_table->assign_a_partition_key(COLOCATE_NAME);
            new_table->pcount = colocated_pcount;
        }
    }
    else{
        if(d6()<4){
            new_table->assign_a_partition_key();
            new_table->pcount = d6();
        }
        // else if(d6()<2){
        //     new_table->fully_replicated = true;
        // }
        // else{
        //     //default
        // }
    }
    ostringstream s;
    new_table->out(s);
    auto sql = s.str();
    ctx -> master_dut -> test(sql);

    ostringstream ss;
    gen->out(ss);
    sql = ss.str() + ";";
    // ctx -> master_dut -> test(sql);
    return sql;
}