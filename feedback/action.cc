#include "action.hh"
#include "../grammar.hh"

void join_by_dkey(joined_table * jt, int table_id_in_group, string dkey, bool joined_on_shard_key){
    if(auto lhs_jt = dynamic_cast<joined_table *>(jt->lhs.get())){
        join_by_dkey(lhs_jt, table_id_in_group, dkey,joined_on_shard_key);
    }
    if(auto rhs_jt = dynamic_cast<joined_table *>(jt->rhs.get())){
        join_by_dkey(rhs_jt, table_id_in_group, dkey,joined_on_shard_key);
    }

    auto lhs_not_subq = (dynamic_cast<table_or_query_name *>(jt->lhs.get()) || dynamic_cast<joined_table *>(jt->lhs.get()));
    auto rhs_not_subq = (dynamic_cast<table_or_query_name *>(jt->rhs.get()) || dynamic_cast<joined_table *>(jt->rhs.get()));

    if(!((lhs_not_subq) && (rhs_not_subq))){
        //todo: deal with subquery?
        return;
    }

    jt->victim_joined_on_shard_key = jt->joined_on_shard_key;
    jt->joined_on_shard_key = joined_on_shard_key;
    // if(joined_on_shard_key == false || dkey == ""){
    //     return;
    // }
    // change table names to target table in the same group
    if(auto ton = dynamic_cast<table_or_query_name *>(jt->lhs.get())){
        ton->victim_joined_on_shard_key = ton->joined_on_shard_key;
        ton->joined_on_shard_key = joined_on_shard_key;
        if(joined_on_shard_key == false || dkey == ""){
            return;
        }
        string table_id_str = ton->t->ident().substr(1);
        int table_group = std::stoi(table_id_str) % total_groups;
        int new_table_id = table_group + table_id_in_group * total_groups;
        string new_table_name = "t" + std::to_string(new_table_id);
        ton -> table_name_after_action = new_table_name;
    }
    if(auto ton = dynamic_cast<table_or_query_name *>(jt->rhs.get())){
        ton->victim_joined_on_shard_key = ton->joined_on_shard_key;
        ton->joined_on_shard_key = joined_on_shard_key;
        if(joined_on_shard_key == false || dkey == ""){
            return;
        }
        string table_id_str = ton->t->ident().substr(1);
        int table_group = std::stoi(table_id_str) % total_groups;
        int new_table_id = table_group + table_id_in_group * total_groups;
        string new_table_name = "t" + std::to_string(new_table_id);
        ton -> table_name_after_action = new_table_name;
    }
    
    string condition = "";
    condition += jt->lhs->refs[0]->ident() + "." + dkey;
    condition += " = ";
    condition += jt->rhs->refs[0]->ident() + "." + dkey;
    jt->shard_key_condition = condition;
}

void query_mutator::visit(struct prod *p){
    if(dynamic_cast<table_or_query_name *>(p)){
        table_or_query_name * t = dynamic_cast<table_or_query_name *>(p);
        string table_name;
        if(""== t->table_name_after_action){
            table_name = t->t->ident();
        } else {
            table_name = t->table_name_after_action;
        }
        bool matched = false;
        if(table_name == victim){
            //t = new table_or_query_name(p, substitute);
            t->table_name_after_action = substitute->name;
            t->assigned_shard = this->assigned_shard;
            t->joined_on_shard_key = this->joined_on_shard_key;
            matched = true;
        }
        else if(victim == "-1" && t->t->ident()[0] == 't') {
            //victim is -1 means any table in the same group
            string table_id_str = t->t->ident().substr(1);
            int table_id = std::stoi(table_id_str);
            if(table_id % total_groups == substitute->get_id_from_name() % total_groups){
                t->table_name_after_action = substitute->name;
                t->assigned_shard = this->assigned_shard;
                t->joined_on_shard_key = this->joined_on_shard_key;
                matched = true;
            }
        }

        if(!matched){
            return;
        }
        // find where clause to assign shard condition
        prod* parent = p->pprod;
        while(dynamic_cast<table_ref *>(parent)){
            parent = parent->pprod;
        }
        if(dynamic_cast<from_clause *>(parent)){
            parent = parent->pprod;
        }
        // parent is query_spec or update_stmt or delete_stmt
        shared_ptr<where_clause> wc = nullptr;
        if(query_spec* qs = dynamic_cast<query_spec *>(parent)){
            wc = qs->search;
        }
        else if(update_stmt* us = dynamic_cast<update_stmt *>(parent)){
            wc = us->search;
        }
        else if(delete_stmt* ds = dynamic_cast<delete_stmt *>(parent)){
            wc = ds->search;
        }
        if(!wc){
            // cout << "Cannot find where clause to assign shard condition of table " << *t << endl;
            return;
        }

        if(assigned_shard){
            // wc->assigned_shard represents whether the last action has assigned shard condition
            auto& dds = wc->scope->schema->ddss[substitute->name];
            string dkey = dds.params["dkey"];
            if(dkey != ""){
                wc->victim_assigned_shard = wc->assigned_shard;
                wc->assigned_shard = true;
                t->victim_assigned_shard = wc->assigned_shard;
                t->assigned_shard = true;
                sqltype* key_type = nullptr;
                for(auto col : substitute->columns()){
                    if(col.name == dkey){
                        key_type = col.type;
                        break;
                    }
                }
                if(!key_type){
                    throw runtime_error("Cannot find dkey column in target table");
                }
                
                wc->assigned_shard_name = make_shared<column_reference>(p, key_type, dkey, t->refs[0]->ident());
                wc->assigned_shard_value = make_shared<const_expr>(p, key_type);
            }
        }else if(wc->assigned_shard == true || t->assigned_shard){
            // need to remove assigned shard condition
            t->victim_assigned_shard = wc->assigned_shard;
            t->assigned_shard = false;
            wc->victim_assigned_shard = wc->assigned_shard;
            wc->assigned_shard = false;
        }

        if(dynamic_cast<joined_table *>(p) && joined_on_shard_key == false){
            auto j = dynamic_cast<joined_table *>(p);
            j->joined_on_shard_key = false;
            return;
        }

        parent = p->pprod;
        if(!dynamic_cast<joined_table *>(parent)){
            // cout << "joined_on_shard_key is true, but table '"<< *t <<"' is not in joined_table" << endl;
            // cout<<*parent<<endl;
            return;
        }
        string table_id_str = substitute->name.substr(1);
        int table_id_in_group = std::stoi(table_id_str) / total_groups;
        auto jt = dynamic_cast<joined_table *>(parent);
        if(jt -> type == "cross"){
            // cout << "joined_on_shard_key is true, but join type is cross "<< *t << endl;
            return;
        }
        auto& dds = t->scope->schema->ddss[substitute->name];
        string dkey = dds.params["dkey"];
        if(joined_on_shard_key && dkey != "colocated_key"){
            // cout << "joined_on_shard_key is true, but dkey_name is not colocated_key: "<< *t << endl;
            return;
        }
        join_by_dkey(jt, table_id_in_group, dkey,joined_on_shard_key);
    }
    if(dynamic_cast<atomic_subselect *>(p)){
        atomic_subselect * subselect = dynamic_cast<atomic_subselect *>(p);
        if(subselect->tab->ident() == victim){
            subselect->tab = substitute;
            string col_name = subselect->col->name;
            auto& cols = substitute->columns(); 
            for (auto& col : cols) {
                if (col.name == col_name) {
                    subselect->col = &col;
                    break;
                }
            }
        }
        else if(victim == "-1" && subselect->tab->ident()[0] == 't') {
            //victim is -1 means any table in the same group
            string table_id_str = subselect->tab->ident().substr(1);
            int table_id = std::stoi(table_id_str);
            if(table_id % total_groups == substitute->get_id_from_name() % total_groups){
                subselect->tab = substitute;
                string col_name = subselect->col->name;
                auto& cols = substitute->columns(); 
                for (auto& col : cols) {
                    if (col.name == col_name) {
                        subselect->col = &col;
                        break;
                    }
                }
            }    
        }
    }
}

vector<dds> get_dds_list(shared_ptr<schema> s){
    vector<dds> dds_list;
    for(auto dds : s->ddss){
        dds_list.push_back(dds.second);
    }
    return dds_list;
}

action get_action_from_server(std::shared_ptr<schema> s, std::unordered_map<std::string, std::string>& params) {
    action a;
    a.victim = params.at("victim");
    a.target = params.at("target");

    table* target_table = nullptr;
    for(auto& t : s->tables) {          //   &
        if (t.name == a.target) {
            target_table = &t;            //  
            break;
        }
    }
    a.qm = std::make_shared<query_mutator>(a.victim, target_table);
    return a;
}
