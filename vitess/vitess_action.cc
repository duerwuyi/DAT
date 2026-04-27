#include "vitess_action.hh"

void vitess_distributor::clear_record_file(){
    namespace fs = std::filesystem;
    const fs::path base{"./vitess/saved"};

    std::error_code ec;

    // 
    if (!fs::exists(base, ec)) {
        fs::create_directories(base, ec);
        if (ec) {
            throw std::runtime_error("clear_record_file: create_directories failed: " + ec.message());
        }
        return;
    }

    //  saved/  saved/ 
    for (fs::directory_iterator it(base, ec), end; !ec && it != end; it.increment(ec)) {
        fs::remove_all(it->path(), ec);
        if (ec) break;
    }

    if (ec) {
        throw std::runtime_error("clear_record_file: remove_all failed: " + ec.message());
    }
}

// void vitess_distributor::ensure_vschemas_inited() {
//     if (!dist_sharded_vschema_inited_) {
//         dist_sharded_vschema_.sharded = true;
//         dist_sharded_vschema_inited_ = true;
//     }
//     if (!single_local_vschema_inited_) {
//         single_local_vschema_.sharded = false;
//         single_local_vschema_inited_ = true;
//     }
// }

vitess_distributor::vitess_distributor(shared_ptr<Context> context): distributor(context){
    vtctldclient_config = read_vtctldclient_config();
    if(vtctldclient_config.count("distributed") == 0)
        throw runtime_error("no distributed in vtctldclient_config");
    vitess_sharded = vtctldclient(vtctldclient_config["distributed"].first, vtctldclient_config["distributed"].second);
    if(vtctldclient_config.count("single") == 0)
        throw runtime_error("no single in vtctldclient_config");
    vitess_single = vtctldclient(vtctldclient_config["single"].first, vtctldclient_config["single"].second);

    //generate vindex for v_distributed
    v_distributed.sharded = true;
    v_distributed.keyspace = ctx->info.test_db;
    v_distributed.vindexes = {
        {"index1",vitess_vindex_def("","xxhash")},
        {"index2",vitess_vindex_def("","unicode_loose_xxhash")},
        {"index3",vitess_vindex_def("","null")},
        // {"index3",vitess_vindex_def("","lookup_unique")},
    };
    v_dist_local.sharded = false;
    v_dist_local.keyspace = "local";
    v_single.sharded = false;
    v_single.keyspace = "local";
}

string vitess_distributor::distribute_one(shared_ptr<prod> gen, int* affect_num, int i){
    auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
    if(create_table == nullptr){
        throw runtime_error("not create_table_stmt");
    }
    int groupid = i % total_groups;
    int id_in_group = i / total_groups;
    string keyspace = ctx->info.test_db; // "sharded"
    string single_keyspace = "local";
    shared_ptr<vitess_table> victim = make_shared<vitess_table>(create_table->created_table->name,
        keyspace,
        create_table->created_table->is_insertable,
        create_table->created_table->is_base_table
    );
    shared_ptr<vitess_table> victim_single = make_shared<vitess_table>(create_table->created_table->name,
        single_keyspace,
        create_table->created_table->is_insertable,
        create_table->created_table->is_base_table
    );
    victim_single->keyspace_sharded = false;
    

    // // lazy init: load current vschema so we don't clobber unrelated settings
    // if (v_distributed.tables.empty()) {
    //     try {
    //         v_distributed.fill_vschema(vitess_sharded.GetVSchemaString(keyspace));
    //     } catch (...) {
    //         // if GetVSchema fails (e.g. empty), keep minimal default
    //         v_distributed.sharded = true;
    //     }
    // }
    // if (v_single.tables.empty()) {
    //     try {
    //         v_single.fill_vschema(vitess_single.GetVSchemaString("local"));
    //     } catch (...) {
    //         v_single.sharded = false;
    //     }
    // }
    string sql = "";
    //generate table instance on distri and single
    bool workflow = false;
    string workflow_command = "";

    // generate dds on victim
    if(id_in_group == 1){
        ostringstream s;
        gen->out(s);
        sql = s.str() + ";";
        //use vtctldclient.ApplySchemaFromString() to apply table
        // distributed cluster: create in sharded keyspace
        vitess_sharded.ApplySchemaFromString(keyspace, sql);
        // single cluster: create in local keyspace
        vitess_single.ApplySchemaFromString("local", sql);

        if(groupid == 0){
            // vector<string> pinned_values = {"10","40","60","80","a0","c0","d0"};
            vector<string> pinned_values = {"00"};
            pinned_local_kid_ = random_pick(pinned_values);
        }
        victim->keyspace_sharded = true;
        victim->column_vindexes.push_back(vitess_column_vindex("index3", "pkey"));
        victim->pinned = pinned_local_kid_;
    }else if(id_in_group == 0){
        // Reference table:
        //   - source:  distributed cluster keyspace "local" (unsharded)
        //   - target:  distributed cluster keyspace `keyspace` (sharded)
        //   - vschema(target): {type:"reference", source:"local.<table>"}
        //   - vschema(source): {type:"reference"}  (optional but recommended)
        //   - workflow: VReplication reference tables (recommended)

        ostringstream s;
        create_table -> constraints[0] = "unique not null";
        gen->out(s);
        sql = s.str() + ";";
        //use vtctldclient.ApplySchemaFromString() to apply table
        // distributed cluster: create in sharded keyspace
        vitess_sharded.ApplySchemaFromString(keyspace, sql);
        // single cluster: create in local keyspace
        vitess_single.ApplySchemaFromString("local", sql);

        // 1) create the same physical table in distributed cluster's local keyspace as the source
        vitess_sharded.ApplySchemaFromString("local", sql);

        // 2) mark target table as reference and bind to source
        victim->keyspace_sharded = true;
        victim->type = vitess_table_type::reference;
        victim->pinned.clear();
        victim->column_vindexes.clear();
        victim->source = "local." + victim->ident();

        // 3) add to local-vschema for distributed cluster "local" keyspace
        auto src = std::make_shared<vitess_table>(
            create_table->created_table->name,
            "local",
            create_table->created_table->is_insertable,
            create_table->created_table->is_base_table
        );
        src->keyspace_sharded = false;
        src->type = vitess_table_type::reference;
        v_dist_local.tables.push_back(src);

        // we must apply local vschema to distributed cluster
        vitess_sharded.ApplyVSchemaFromString("local", v_dist_local.to_json());

        // 4) prepare workflow name (create after ApplyVSchema)
        auto sanitize_wf = [](std::string s) {
            for (char& c : s) {
                if (!(std::isalnum((unsigned char)c) || c=='_' || c=='-')) c = '_';
            }
            return s;
        };

        string pending_ref_table = victim->ident();
        string pending_ref_workflow = "ref_" + sanitize_wf(victim->ident()) + "_" + std::to_string(i);
        workflow_command = "Materialize --target-keyspace "+ keyspace
        + " --workflow " + pending_ref_workflow
        +" create --source-keyspace local --reference-tables " + pending_ref_table;
        workflow = true;
    }else if(id_in_group == 2){
        if(groupid != 0){
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");
            ostringstream s;
            gen->out(s);
            sql = s.str() + ";";
            //use vtctldclient.ApplySchemaFromString() to apply table
            // distributed cluster: create in sharded keyspace
            vitess_sharded.ApplySchemaFromString(keyspace, sql);
            // single cluster: create in local keyspace
            vitess_single.ApplySchemaFromString("local", sql);

            victim->keyspace_sharded = true;
            victim->type = vitess_table_type::normal;
            victim->pinned.clear();
            victim->column_vindexes.push_back(vitess_column_vindex(colocate_vindex, COLOCATE_NAME));
        }else{
            //the first time for colocated tables
            vector<sqltype *> enable_type;
            enable_type.push_back(gen->scope->schema->inttype);
            enable_type.push_back(gen->scope->schema->texttype);
            enable_type.push_back(gen->scope->schema->realtype);
            enable_type.push_back(gen->scope->schema->datetype);
            auto type = random_pick<>(enable_type);

            colocate_column = make_shared<column>(COLOCATE_NAME, type);
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");

            ostringstream s;
            gen->out(s);
            sql = s.str() + ";";
            //use vtctldclient.ApplySchemaFromString() to apply table
            // distributed cluster: create in sharded keyspace
            vitess_sharded.ApplySchemaFromString(keyspace, sql);
            // single cluster: create in local keyspace
            vitess_single.ApplySchemaFromString("local", sql);

            vector<string> vindex_names ={"index1","index2"};
            auto vindex_name = random_pick(vindex_names);
            // colocate_vindex = make_shared<pair<string,vitess_vindex_def>>(make_pair(it->first,it->second));
            colocate_vindex = vindex_name;
            victim->keyspace_sharded = true;
            victim->type = vitess_table_type::normal;
            victim->pinned.clear();
            victim->column_vindexes.push_back(vitess_column_vindex(vindex_name, COLOCATE_NAME));
        }
    }
    // else if(id_in_group == 3){
        
    // }
    else{
        ostringstream s;
        gen->out(s);
        sql = s.str() + ";";
        //use vtctldclient.ApplySchemaFromString() to apply table
        // distributed cluster: create in sharded keyspace
        vitess_sharded.ApplySchemaFromString(keyspace, sql);
        // single cluster: create in local keyspace
        vitess_single.ApplySchemaFromString("local", sql);
        vector<string> vindex_names ={"index1","index2"};
        auto vindex_name = random_pick(vindex_names);
        auto col = random_pick(create_table->created_table->columns());
        // colocate_vindex = make_shared<pair<string,vitess_vindex_def>>(make_pair(it->first,it->second));
        colocate_vindex = vindex_name;
        victim->keyspace_sharded = true;
        victim->type = vitess_table_type::normal;
        victim->pinned.clear();
        victim->column_vindexes.push_back(vitess_column_vindex(vindex_name, col.name));
    }
    //add victim to v_distributed and v_single. v_single's keyspace is "local".
    v_distributed.tables.push_back(victim);
    v_single.tables.push_back(victim_single);
    //use vtctldclient.ApplyVSchemaFromString() to apply vschema
    vitess_sharded.ApplyVSchemaFromString(keyspace, v_distributed.to_json());
    vitess_single.ApplyVSchemaFromString("local", v_single.to_json());
    //workflow for reference tables
    if(workflow) vitess_sharded.Exec_Command(workflow_command);
    if (affect_num) {
        // DDL row count not meaningful here; just report 1 newly created table.
        *affect_num = 1;
    }
    cout<<sql<<endl;
    return sql;
}