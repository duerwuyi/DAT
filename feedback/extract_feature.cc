#include "extract_feature.hh"
#include "../grammar.hh"
#include "../globals.h"
#include "queue"

template <class T>
static inline void push_back_unique(std::vector<T>& v, const T& x) {
    if (std::find(v.begin(), v.end(), x) == v.end()) v.push_back(x);
}

//  get-or-create

static inline std::string feature_join_label(joined_table* jt) {
    std::string label = feature_use_join_type ? jt->type : "join";
    if (feature_use_colocated_join && jt->joined_on_shard_key) {
        label += "_colocated";
    }
    return label;
}

static inline std::string feature_setop_label(unioned_query* uq) {
    return feature_use_setop_type ? uq->type : "setop";
}

static inline table_type_info feature_table_info(table_type_info t) {
    if (!feature_use_table_distribution) {
        t.type = 0;
    }
    if (!feature_use_shard_routing) {
        t.assigned_shard = false;
        t.joined_on_shard_key = false;
    }
    return t;
}

static inline std::string feature_subquery_label(const std::string& label) {
    return feature_use_subquery_type ? label : "subquery";
}

static bool query_relation_id(struct prod* p, int& id) {
    if (auto* qs = dynamic_cast<query_spec *>(p)) {
        id = qs->relation_id;
        return true;
    }
    if (auto* ms = dynamic_cast<modifying_stmt *>(p)) {
        id = ms->relation_id;
        return true;
    }
    if (auto* uq = dynamic_cast<unioned_query *>(p)) {
        id = uq->relation_id;
        return true;
    }
    return false;
}

static bool relation_id_for_subquery_target(struct prod* p, int& id) {
    if (query_relation_id(p, id)) {
        return true;
    }
    if (auto* jt = dynamic_cast<joined_table *>(p)) {
        id = jt->relation_id;
        return true;
    }
    if (auto* tr = dynamic_cast<table_ref *>(p)) {
        id = tr->relation_id;
        return true;
    }
    return false;
}

static bool nearest_outer_query_relation_id(struct prod* p, int& id) {
    for (auto* cur = p ? p->pprod : nullptr; cur != nullptr; cur = cur->pprod) {
        if (query_relation_id(cur, id)) {
            return true;
        }
    }
    return false;
}

inline ast_relation& upsert_node(std::map<int, ast_relation>& M, struct prod* p, int id) {
    auto [it, inserted] = M.try_emplace(id);  // 
    ast_relation& node = it->second;
    if (inserted) {                           // 
        node.id        = id;
        // node.parent_id = p->pprod ? p->pprod->scope->relation_id : -1;
        node.is_terminal = false;
        node.prod_ptr  = p;
    }
    else {
        // 
        // if (node.parent_id == -1 && p->pprod)
        //     node.parent_id = p->pprod->scope->relation_id;
        // node.prod_ptr 
        node.id = id;
        node.prod_ptr  = p;
    }
    return node;
}

static void add_subquery_relation(std::map<int, ast_relation>& ast_relations,
                                  struct prod* subquery_node,
                                  struct prod* inner_query,
                                  const std::string& label) {
    int outer_id = -1;
    int inner_id = -1;
    if (!nearest_outer_query_relation_id(subquery_node, outer_id) ||
        !relation_id_for_subquery_target(inner_query, inner_id)) {
        return;
    }

    const std::string op = feature_subquery_label(label);
    push_back_unique(ast_relations[outer_id].operator_relations[op], inner_id);
    push_back_unique(ast_relations[inner_id].operator_relations[op + "_parent"], outer_id);
}

void extracter::visit(struct prod *p) {
    if (dynamic_cast<query_spec *>(p) ||
        dynamic_cast<modifying_stmt *>(p) ||
        dynamic_cast<joined_table *>(p)   ||
        dynamic_cast<unioned_query *>(p)) {
        int id = -1;
        if (auto* qs = dynamic_cast<query_spec *>(p))
            id = qs->relation_id;
        else if (auto* ms = dynamic_cast<modifying_stmt *>(p))
            id = ms->relation_id;
        else if (auto* jt = dynamic_cast<joined_table *>(p))
            id = jt->relation_id;
        else if (auto* uq = dynamic_cast<unioned_query *>(p))
            id = uq->relation_id;
        else
            assert(false && "Unreachable code in extracter::visit");

        auto& node = upsert_node(ast_relations, p, id); // <<<  + 

        if (auto* jt = dynamic_cast<joined_table *>(p)) {
            int lhs_id = jt->lhs->relation_id;
            int rhs_id = jt->rhs->relation_id;

            const string join_label = feature_join_label(jt);

            // 
            push_back_unique(ast_relations[node.id]
                             .operator_relations[join_label], lhs_id);
            push_back_unique(ast_relations[node.id]
                             .operator_relations[join_label], rhs_id);

            // 
            push_back_unique(ast_relations[lhs_id]
                             .operator_relations[join_label + "_parent"], node.id);
            push_back_unique(ast_relations[rhs_id]
                             .operator_relations[join_label + "_parent"], node.id);
        }
        else if (auto* ms = dynamic_cast<modifying_stmt *>(p)) {
            if(!ms->victim){
                cout << "modifying_stmt has no victim table! stmt type: "<< typeid(*ms).name() << endl;
                return;
            }
            // 
            node.modify = ms->victim;
            auto& victim_node = upsert_node(ast_relations, p, ms->relation_id_for_victim_table);
            victim_node.is_terminal = true;
            victim_node.modify = ms->victim;
            string modify_type = "";
            if(dynamic_cast<insert_select_stmt *>(ms)){
                modify_type = "insert_select";
            }
            else if(dynamic_cast<delete_stmt *>(ms)){
                modify_type = "delete";
            }
            else if(dynamic_cast<update_stmt *>(ms)){
                modify_type = "update";
            }
            else if(dynamic_cast<merge_stmt *>(ms)){
                modify_type = "merge";
            }else{
                modify_type = "modify";
            }
            if (!feature_use_modify_type) {
                modify_type = "modify";
            }
            
            const std::string tbl =
                ms->mutated ? ms->table_name_after_action : ms->victim->ident();
            table* alias = new table(tbl,"",true,true);
            if (victim_node.reference_name.empty())
                victim_node.reference_name = alias->ident();
            victim_node.alias_ptr = alias;
            victim_node.terminal_table_name = tbl;
            table_id_to_ast_relation_id.emplace(tbl, victim_node.id);
            used_table_names.push_back(tbl);

            push_back_unique(node.operator_relations[modify_type], victim_node.id);
            push_back_unique(victim_node.operator_relations[modify_type + "_by"], node.id);
        }
        else if (auto* qs = dynamic_cast<query_spec *>(p)) {
            // FROM  made_from 
            if (feature_use_query_block) {
                for (auto& ref : qs->from_clause->reflist) {
                    int ref_id = ref->relation_id;
                    push_back_unique(node.operator_relations["made_from"], ref_id);
                    push_back_unique(ast_relations[ref_id].operator_relations["compose"], node.id);
                }
            }
            // used_by 
        }
        else if (auto* uq = dynamic_cast<unioned_query *>(p)) {
            int lhs_id = uq->lhs->relation_id;
            int rhs_id = uq->rhs->relation_id;

            const string setop_label = feature_setop_label(uq);

            push_back_unique(ast_relations[node.id]
                             .operator_relations[setop_label], lhs_id);
            push_back_unique(ast_relations[node.id]
                             .operator_relations[setop_label], rhs_id);

            push_back_unique(ast_relations[lhs_id]
                             .operator_relations[setop_label + "_parent"], node.id);
            push_back_unique(ast_relations[rhs_id]
                             .operator_relations[setop_label + "_parent"], node.id);
        }

        // cout <<"non-ter "<< node.id << ": ";
        // ast_relations[node.id].print();
    }
    else if (auto* ep = dynamic_cast<exists_predicate *>(p)) {
        add_subquery_relation(ast_relations, p, ep->subquery.get(), "exists_subquery");
    }
    else if (auto* iq = dynamic_cast<in_query *>(p)) {
        add_subquery_relation(ast_relations, p, iq->in_subquery.get(), "in_subquery");
    }
    else if (auto* cs = dynamic_cast<comp_subquery *>(p)) {
        add_subquery_relation(ast_relations, p, cs->target_subquery.get(), "comp_subquery");
    }
    else if (auto* ts = dynamic_cast<table_subquery *>(p)) {
        add_subquery_relation(ast_relations, p, ts->query.get(), ts->is_lateral ? "lateral_subquery" : "from_subquery");
    }
    // 
    else if (auto* ton = dynamic_cast<table_or_query_name *>(p)) {
        // cout<<"Found table_or_query_name: "<< *p <<", assign_shard: "<< ton->assigned_shard << endl;
        auto& node = upsert_node(ast_relations, p, ton->relation_id); // <<< 

        node.is_terminal = true; //  false  OR
        // 
        if (node.reference_name.empty())
            node.reference_name = ton->refs[0]->ident();
        node.alias_ptr = ton->refs[0].get();

        const std::string tbl =
            ton->table_name_after_action.empty() ? ton->t->ident()
                                                 : ton->table_name_after_action;
        node.terminal_table_name = tbl;
        if(first_table_name == "") first_table_name = tbl;

        node.assigned_shard = ton->assigned_shard;
        node.joined_on_shard_key = ton->joined_on_shard_key;

        node.victim_assigned_shard = ton->victim_assigned_shard;
        node.victim_joined_on_shard_key = ton->victim_joined_on_shard_key;

        // 
        table_id_to_ast_relation_id.emplace(tbl, node.id);

        string real_name = ton->table_name_after_action.empty() ? ton->t->ident() : ton->table_name_after_action;
        used_table_names.push_back(real_name);

        // cout  <<"terminal "<< node.id << ": "<< *ast_relations[node.id].prod_ptr
        //     <<", assigned_shard: "<< ast_relations[node.id].assigned_shard << endl;
        // ast_relations[node.id].print();
    }
    // else if (dynamic_cast<atomic_subselect *>(p)) {
    //     atomic_subselect * subselect = dynamic_cast<atomic_subselect *>(p);
    //     string table_name = subselect->tab->ident();
    //     used_table_names.push_back(table_name);
    // }
}

struct column_extracter : public prod_visitor {
    std::map<int, ast_relation>& ast_relations;
    // O(1)  -> ast_relation_id
    std::unordered_map<named_relation*, int> alias2rel;

    column_extracter(std::map<int, ast_relation>& R) : ast_relations(R) {
        alias2rel.reserve(R.size());
        for (auto& kv : R) {
            const ast_relation& ar = kv.second;
            if (ar.is_terminal && ar.alias_ptr) {
                alias2rel.emplace(ar.alias_ptr, ar.id);
            }
        }
    }

    void visit(struct prod* p) override {
        auto* col_ref = dynamic_cast<column_reference*>(p);
        if (!col_ref) return;

        struct prod* ptr = p;
        while(!dynamic_cast<query_spec *>(ptr->pprod) &&
              ptr->pprod != nullptr) {
            ptr = ptr->pprod;
        }
        if (ptr->pprod == nullptr) {
            cout<< "column_reference not in a query_spec: " << *col_ref << endl;
            return;
        }
        auto* qs = dynamic_cast<query_spec *>(ptr->pprod);
        const int qs_id = qs->relation_id;

        // a.x 
        // 
        if (col_ref->table_ref.empty()) return;

        //  outer_refs 
        named_relation* matched_outref = nullptr;
        for (auto* outref : qs->from_clause->outer_refs) {
            if (outref && outref->ident() == col_ref->table_ref) {
                matched_outref = outref;
                break;
            }
        }
        if (!matched_outref) return; // 

        //  alias2rel O(1)  terminal  id
        int target_id = -1;
        auto itId = alias2rel.find(matched_outref);
        if(itId != alias2rel.end()) target_id = itId->second;
        else if (col_ref->table_ref[0]=='t'){
            //only modifying stmt
            for(auto& r : ast_relations){
                if(r.second.reference_name == col_ref->table_ref){
                    target_id = r.first;
                    break;
                }
            }
            if(target_id == -1) return; // 
        }
        else return; // 


        //  <-column_ref_by-  -use_outer-> 
        if (feature_use_outer_ref) {
            push_back_unique(ast_relations[target_id].operator_relations["column_ref_by"], qs_id);
            push_back_unique(ast_relations[qs_id].operator_relations["use_outer"], target_id);
        }

        // 
        // ast_relations[qs_id].block_flags.has_corr_subq = true;

        //  JOIN  ON  p->pprod  joined_table*
        //  col_ref  lhs  rhs  capture_side = Left/Right
    }
};

vector<relation_chain> find_relation_chain(string source_table_name,
    map<int, ast_relation>& ast_relations,
    multimap<string, int>& table_name_to_ast_relation_id,
    map<string, dds> table_dds_info,
    function<table_type_info(dds)> get_table_type_info
){
    //find all source_table_id in ast_relations
    vector<int> source;
    map<int,bool> source_assigned_shard;
    map<int,bool> source_joined_on_shard_key;
    for(auto& r: ast_relations){
        if(r.second.is_terminal && r.second.terminal_table_name == source_table_name){
            source.push_back(r.first);
            source_assigned_shard[r.first] = r.second.assigned_shard;
            source_joined_on_shard_key[r.first] = r.second.joined_on_shard_key;
        }
    }
    table_type_info source_table_type_info{};
    auto it = table_dds_info.find(source_table_name);
    if (it != table_dds_info.end()) {
        if (!get_table_type_info) {
            throw std::runtime_error("get_table_type_info is not set");
        } else {
            source_table_type_info = get_table_type_info(it->second);
        }
    }
    vector<relation_chain> result;
    for(const int s: source){
        //do bfs from s
        int size = ast_relations.size();
        map<int, bool> visited;
        //restore backtrace
        map<int, int> p;
        map<int, string> op_types;
        for(auto& r: ast_relations){
            visited[r.first] = false;
            p[r.first] = -1;
            op_types[r.first] = "";
        }
        queue<int> q;
        q.push(s);
        visited[s] = true;
        p[s] = -1;
        while(!q.empty()){
            int curr = q.front();
            q.pop();
            for(auto& op_rel : ast_relations[curr].operator_relations){
                string op_type = op_rel.first;
                for(auto neighbor : op_rel.second){
                    if(!visited[neighbor]){
                        visited[neighbor] = true;
                        q.push(neighbor);
                        p[neighbor] = curr;// neighbor is visited from curr
                        op_types[neighbor] = op_type;
                    }
                }
            }
        }
        //extract relation_chain
        for(auto& r: ast_relations){
            int curr = r.first;
            if(r.second.is_terminal && visited[curr] && curr != s){
                //found a terminal node
                relation_chain rc;
                //fill rc.from
                rc.from = source_table_type_info;
                rc.from.assigned_shard = source_assigned_shard[s];
                rc.from.joined_on_shard_key = source_joined_on_shard_key[s];
                rc.from = feature_table_info(rc.from);

                table_type_info dst_table_type_info{};
                string dst_table_name = r.second.terminal_table_name;
                auto it = table_dds_info.find(dst_table_name);
                if (it != table_dds_info.end()) {
                    if (!get_table_type_info) {
                        throw std::runtime_error("get_table_type_info is not set");
                    } else {
                        //dst should act as rc.to
                        dst_table_type_info = get_table_type_info(it->second);
                        dst_table_type_info.assigned_shard = r.second.assigned_shard;
                        dst_table_type_info.joined_on_shard_key = r.second.joined_on_shard_key;
                        rc.to = feature_table_info(dst_table_type_info);
                    }
                }
                for(int v = curr; p[v] != -1; v = p[v]){
                    rc.operator_types.push_back(op_types[v]);
                }
                // action changes the tti of the start node s
                rc.victim_assigned_shard = feature_use_shard_routing && ast_relations[s].victim_assigned_shard;
                rc.victim_joined_on_shard_key = feature_use_shard_routing && ast_relations[s].victim_joined_on_shard_key;
                std::reverse(rc.operator_types.begin(), rc.operator_types.end());
                result.push_back(rc);
            }
        }
    }
    return result;
}

int find_query_type(struct prod *p){
    if (!feature_use_query_type) {
        return 0;
    }
    if(dynamic_cast<query_spec *>(p)){
        return 0;
    }
    else if(dynamic_cast<insert_select_stmt *>(p)){
        return 1;
    }
    else if(dynamic_cast<delete_stmt *>(p)){
        return 2;
    }
    else if(dynamic_cast<update_stmt *>(p)){
        return 3;
    }else if(dynamic_cast<merge_stmt *>(p)){
        return 4;
    }
    else if(dynamic_cast<common_table_expression *>(p)){
        return 5;
    }
    return -1;
};

// table_type_info electing_target_in_tree(
//     map<int, shared_ptr<invalid_tree_node>>& invalid_tree_root,
//     prod * p,
//     vector<relation_chain> relation_chains,
//     table_type_info victim
// ){
//     const int qt = find_query_type(p);
//     auto& root_sp = invalid_tree_root[qt];
//     if (!root_sp) root_sp = make_shared<invalid_tree_node>(); // 
//     map<int,int> candidate_ballot;
//     for(auto& rc : relation_chains){
//         std::shared_ptr<invalid_tree_node> cur = root_sp;

//         // 2.1 / table_type_info
//         std::shared_ptr<invalid_tree_node> start = nullptr;
//         for (auto& child : cur->children) {
//             if (!child->is_terminal && child->is_start && child->table_info == rc.from) {
//                 // cout<< "start found: "<< rc.from.type << endl;
//                 start = child;
//                 break;
//             }
//         }
//         if (!start) {
//             //not found, then this relation chain is valid
//             // cout<< "start not found" << endl;
//             continue;
//         }
//         cur = start;
        
//         bool ok = true;
//         for (const auto& op : rc.operator_types) {
//             std::shared_ptr<invalid_tree_node> nxt = nullptr;
//             for (auto& child : cur->children) {
//                 if (!child->is_terminal && !child->is_start && child->operator_type == op) {
//                     // cout<< "operator found: "<< op << endl;
//                     nxt = child;
//                     break;
//                 }
//             }
//             if (!nxt) {
//                 //not found, then this relation chain is valid
//                 // cout<< "operator"<<op<<" not found" << endl;
//                 ok = false; 
//                 break;
//             }
//             cur = nxt;
//         }
//         if (!ok) continue;
//         shared_ptr<invalid_tree_node> parent_of_term = cur; 

//         std::shared_ptr<invalid_tree_node> term = nullptr;
//         for (auto& child : cur->children) {
//             if (child->is_terminal && child->table_info == rc.to) {
//                 // cout<< "terminal found: "<< rc.to.type << endl;
//                 term = child;
//                 break;
//             }
//         }
//         if (!term) {
//             //not found, then this relation chain is valid
//             // cout<< "terminal not found" << endl;
//             continue;
//         }
//         int candidate = term -> vote_for(parent_of_term.get());
//         if(candidate != -1)
//             candidate_ballot[candidate] += 1;
//         // // 2.4 
//         // const double invalidity = term->occurences ? (double)term->ivalid / term->occurences : 0.0;
//         // if (term->occurences >= occ_threshold && invalidity >= inval_threshold) {
//         //     // 
//         //     res = false;
            
//         //     //  rc 
//         //     cout << "invalid relation chain found: ";
//         //     rc.print();

//         //     return res;
//         // }
//     }
//     int leader = -1;
//     for(auto& p: candidate_ballot){
//         if(leader == -1 || p.second > candidate_ballot[leader]){
//             leader = p.first;
//         }
//     }
//     return int_to_table_type_info(leader);
// }

int table_type_info_to_int(table_type_info t) {
        return t.type + (t.assigned_shard ? 100 : 0) + (t.joined_on_shard_key ? 1000 : 0);
}

table_type_info int_to_table_type_info(int v) {
    table_type_info t;
    t.joined_on_shard_key = (v >= 1000);
    if (t.joined_on_shard_key) v -= 1000;
    t.assigned_shard = (v >= 100);
    if (t.assigned_shard) v -= 100;
    t.type = v;
    return t;
}

bool search_invalidity_in_tree(
    map<int, shared_ptr<invalid_tree_node>>& invalid_tree_root,
    prod * p,
    vector<relation_chain> relation_chains,
    int occ_threshold,
    double inval_threshold // if more than threshold of relation chains are invalid, then we consider the query is invalid
){
    const int qt = find_query_type(p);
    auto& root_sp = invalid_tree_root[qt];
    if (!root_sp) root_sp = make_shared<invalid_tree_node>(); // 
    bool res = true;
    for(auto& rc : relation_chains){
        // cout<<"searching :";
        rc.print();
        std::shared_ptr<invalid_tree_node> cur = root_sp;

        // 2.1 / table_type_info
        std::shared_ptr<invalid_tree_node> start = nullptr;
        for (auto& child : cur->children) {
            if (!child->is_terminal && child->is_start && child->table_info == rc.from) {
                // cout<< "start found: "<< rc.from.type <<" ";
                start = child;
                break;
            }
        }
        if (!start) {
            //not found, then this relation chain is valid
            // cout<< "start not found" << endl;
            continue;
        }
        cur = start;
        
        bool ok = true;
        for (const auto& op : rc.operator_types) {
            std::shared_ptr<invalid_tree_node> nxt = nullptr;
            for (auto& child : cur->children) {
                if (!child->is_terminal && !child->is_start && child->operator_type == op) {
                    // cout<< "operator found: "<< op << " ";
                    const double invalidity = child->occurences ? (double)child->ivalid / child->occurences : 0.0;
                    if (child->occurences >= occ_threshold && invalidity >= inval_threshold && prefix_matching) {
                        res = false;
                        //  rc 
                        cout << "invalid relation chain found: ";
                        rc.print();
                        return res;
                    }
                    nxt = child;
                    break;
                }
            }
            if (!nxt) {
                //not found, then this relation chain is valid
                // cout<< "operator: "<<op<<" not found" << endl;
                ok = false; 
                break;
            }
            cur = nxt;
        }
        if (!ok) continue;

        std::shared_ptr<invalid_tree_node> term = nullptr;
        for (auto& child : cur->children) {
            if (child->is_terminal && child->table_info == rc.to) {
                // cout<< "terminal found: "<< rc.to.type << endl;
                term = child;
                break;
            }
        }
        if (!term) {
            //not found, then this relation chain is valid
            // cout<< "terminal not found" << endl;
            continue;
        }

        // 2.4 
        const double invalidity = term->occurences ? (double)term->ivalid / term->occurences : 0.0;
        if (term->occurences >= occ_threshold && invalidity >= inval_threshold) {
            // 
            res = false;
            
            //  rc 
            cout << "invalid relation chain found: ";
            rc.print();

            return res;
        }
    }
    return res;
}

bool add_relation_chain_to_tree(
    std::map<int, std::shared_ptr<invalid_tree_node>>& roots,
    prod* p,
    std::vector<relation_chain>& relation_chains,
    bool is_valid,
    table_type_info victim,
    bool is_unique_query_plan,
    int occ_threshold,
    double inval_threshold // e.g. 1.0 0.9 90%+
){
    const int key = find_query_type(p);

    // 1)  map  shared_ptr
    auto& root_sp = roots[key];
    if (!root_sp) {
        root_sp = std::make_shared<invalid_tree_node>(); // 
        // root_sp->is_start/is_terminal  false
    }

    bool res = true;

    for (auto& rc : relation_chains) {
        // 2) 
        std::shared_ptr<invalid_tree_node> cur = root_sp;

        // 2.1 / table_type_info
        std::shared_ptr<invalid_tree_node> start = nullptr;
        for (auto& child : cur->children) {
            if ((!child->is_terminal) && child->is_start && child->table_info == rc.from) {
                start = child;
                // cout<<"start found: "<<start->table_info.type<<"; ";
                break;
            }
        }
        if (!start) {
            start = std::make_shared<invalid_tree_node>();
            start->is_start    = true;
            start->is_terminal = false;
            start->table_info  = rc.from;     //  flag
            start->occurences  = 0;
            start->ivalid      = 0;
            cur->children.push_back(start);
            // cout<<"start inserted: "<<start->table_info.type<<"; ";
        }
        // 
        // start->occurences += 1;
        // if (!is_valid) start->ivalid += 1;

        cur = start;

        // 2.2 / operator 
        for (const auto& op : rc.operator_types) {
            std::shared_ptr<invalid_tree_node> nxt = nullptr;
            for (auto& child : cur->children) {
                if ((!child->is_terminal) && (!child->is_start) && child->operator_type == op) {
                    nxt = child;
                    // cout<<"op found: "<<op<<"; ";
                    break;
                }
            }
            if (!nxt) {
                nxt = std::make_shared<invalid_tree_node>();
                nxt->is_start      = false;
                nxt->is_terminal   = false;
                nxt->operator_type = op;
                nxt->occurences    = 0;
                nxt->ivalid        = 0;
                cur->children.push_back(nxt);
                // cout<<"op inserted: "<<op<<"; ";
            }
            nxt->occurences += 1;
            if (!is_valid) nxt->ivalid += 1;

            cur = nxt;
        }

        // 2.3  table_type_info
        std::shared_ptr<invalid_tree_node> term = nullptr;
        for (auto& child : cur->children) {
            if (child->is_terminal && child->table_info == rc.to) {
                term = child;
                // cout<<"term found: "<<term->table_info.type<<"; ";
                break;
            }
        }
        if (!term) {
            term = std::make_shared<invalid_tree_node>();
            term->is_start     = false;
            term->is_terminal  = true;
            term->table_info   = rc.to;   //  flag
            term->occurences   = 0;
            term->ivalid       = 0;
            cur->children.push_back(term);
            // cout<<"term inserted: "<<term->table_info.type<<"; ";
        }
        term->occurences += 1;
        if (!is_valid) {
            term->ivalid += 1;
        }else{
            // int victim_number = table_type_info_to_int(victim) + rc.victim_assigned_shard ? 100 : 0 + rc.victim_joined_on_shard_key ? 1000 : 0;
            // term->total_plan_each_victim[victim_number] += 1;
            // cout<< "increment plan count for victim type "<< victim_number << endl;
            // if(is_unique_query_plan)
            //     term->unique_plan_each_victim[victim_number] += 1;
        }
        // cout<<"occ:"<<term->occurences<<", invalid:"<<term->ivalid<<endl;

        // 2.4 
        const double invalidity = term->occurences ? (double)term->ivalid / term->occurences : 0.0;
        if (term->occurences >= occ_threshold && invalidity >= inval_threshold) {
            // 
            res = false;
            //  rc 
            if(is_valid)
                cout << "a chain considered invalid is valid in this query: ";
            else
                cout << "new invalid relation chain: ";
            rc.print();
            cout<<"occ: "<<term->occurences<<", invalid count: "<<term->ivalid<<endl;
        }
    }

    return res;
}

query_feature extracter::extract_feature(shared_ptr<schema> s, query &q){
    //visit(q.ast.get());
    q.ast -> accept(this);
    // for(auto& it: ast_relations){
    //     it.second.print();
    // }
    //extract column reference information
    column_extracter col_extractor(ast_relations);
    q.ast -> accept(&col_extractor);

    // for(auto& it: ast_relations){
    //     it.second.print();
    // }
    
    //joined table: if((a join b) join c), then a,b joined_with c?

    // for(auto u : usages){
    //     // cout<< "table: " << u.table_name << ", from_clause_id: " << u.from_clause_id << ", depth: " << u.depth << endl;
    // }
    // int total_table_num = s -> tables.size();
    // feature.usages = vector<vector<int>>(total_table_num, vector<int>(max_from_clause_id + 1, 0));
    // for(auto usage : usages){
    //     int i = (usage.table_ptr->get_id_from_name());
    //     feature.usages[i][usage.from_clause_id] = usage.depth;
    // }
    // //maybe we should extract some feature from query plan
    // feature.query_plan = q.get_query_plan();
    // feature.is_valid = q.is_valid;
    query_feature feature;
    return feature;
}

vector<int> extracter::used_table(){
    set<int> used = used_table_sets();
    return vector<int>(used.begin(), used.end());
}

set<int> extracter::used_table_sets(){
    set<int> used;
    // cout<<"used_name set size: "<< used_table_names.size() << endl;
    for(auto u : used_table_names){
        if(u[0] == 't')
        used.insert(std::stoi(u.substr(1)));
    }
    // cout<<"used set size: "<< used.size() << endl;
    return used;
}

vector<action> extracter::avalilable_actions(shared_ptr<schema> s,vector<double>& weights, vector<action> excluded_actions, int victim){
    vector<action> actions;
    weights.clear();
    vector<int> used;
    if(victim != -1){
        used.push_back(victim);
    }else{
        used = used_table();
    }
    // cout<<"used tables size: "<< used.size() << endl;
    for(auto u : used){
        // cout<<"aval_from: "<< u<<endl;
        int group_id = u % total_groups;
        int total_table_num = s -> tables.size();
        // for(int i = group_id + total_groups; i < total_table_num; i += total_groups){
        for(int i = group_id; i < total_table_num; i += total_groups){
            string victim = "t" + std::to_string(u); //any table in the same group
            // any table in the same group except itself
            string target = "t" + std::to_string(i);
            table_type_info tti = get_table_type_info_from_dds(s->target_ddbms)(s->ddss[target]);

            bool excluded = false;
            for(auto& ex_a : excluded_actions){
                if(ex_a.victim == victim 
                    && ex_a.target == "t" + std::to_string(i) 
                ){
                    excluded = true;
                    break;
                }
            }
            if(i == u || excluded){
                continue;
            }else if(tti.type == 2){ //distributed table
                for(int j = 0; j < 4; j++){
                    action a;
                    int addition = 0;
                    a.victim = victim;
                    a.target = target;
                    a.assigned_shard = j > 1;            // a0 = false, a1 = false, a2 = true, a3 = true
                    if(a.assigned_shard) addition += 5;
                    if(i / total_groups == 2){ //colocate table
                        a.joined_on_shard_key = j % 2 == 1;  // a0 = false, a1 = true, a2 = false, a3 = true
                        if(a.joined_on_shard_key) if(a.assigned_shard) addition += 20;
                    }else{
                        a.joined_on_shard_key = false;
                    }
                    a.qm = make_shared<query_mutator>(a.victim, a.find_target_table(s), a.assigned_shard, a.joined_on_shard_key);
                    actions.push_back(a);
                    weights.push_back(1.0);
                }
            }else{
                action a;
                a.victim = victim;
                a.target = target;
                a.assigned_shard = false;
                a.joined_on_shard_key = false;
                a.qm = make_shared<query_mutator>(a.victim, a.find_target_table(s), a.assigned_shard, a.joined_on_shard_key);
                actions.push_back(a);
                weights.push_back(1.0);
            }
        }
    }
    return actions;
}

vector<action> extracter::avalilable_actions2(shared_ptr<schema> s,vector<string> candidate_targets, vector<action> excluded_actions
, bool assigned_shard, bool joined_on_shard_key){
    vector<action> actions;
    vector<int> used = used_table();
    // cout<<"used tables size: "<< used.size() << endl;
    for(auto u : used){
        int group_id = u % total_groups;
        int total_table_num = s -> tables.size();
        for(int i = group_id + total_groups; i < total_table_num; i += total_groups){
            string victim = "t" + std::to_string(u); //any table in the same group
            // any table in the same group except itself
            string target = "t" + std::to_string(i);
            if(std::find(candidate_targets.begin(), candidate_targets.end(), target) == candidate_targets.end()){
                continue;
            }

            table_type_info tti = get_table_type_info_from_dds(s->target_ddbms)(s->ddss[target]);

            bool excluded = false;
            for(auto& ex_a : excluded_actions){
                if(ex_a.victim == victim 
                    && ex_a.target == "t" + std::to_string(i) 
                ){
                    excluded = true;
                    break;
                }
            }
            if(i == u || excluded){
                continue;
            }else{
                action a;
                a.victim = victim;
                a.target = target;
                a.assigned_shard = tti.type == 2 ? assigned_shard : false;
                a.joined_on_shard_key = tti.type == 2 ? joined_on_shard_key : false;
                a.qm = make_shared<query_mutator>(a.victim, a.find_target_table(s), a.assigned_shard, a.joined_on_shard_key);
                actions.push_back(a);
            }
        }
    }
    return actions;
}

vector<action> extracter::avalilable_actions3(shared_ptr<schema> s,vector<double>& weights, vector<action> excluded_actions, int victim){
    vector<action> actions;
    weights.clear();
    vector<int> used;
    if(victim != -1){
        used.push_back(victim);
    }else{
        used = used_table();
    }
    // cout<<"used tables size: "<< used.size() << endl;
    for(auto u : used){
        // cout<<"aval_from: "<< u<<endl;
        int group_id = u % total_groups;
        int total_table_num = s -> tables.size();
        // for(int i = group_id + total_groups; i < total_table_num; i += total_groups){
        for(int i = group_id; i < total_table_num; i += total_groups){
            string victim = "t" + std::to_string(u); //any table in the same group
            // any table in the same group except itself
            string target = "t" + std::to_string(i);
            table_type_info tti = get_table_type_info_from_dds(s->target_ddbms)(s->ddss[target]);

            bool excluded = false;
            for(auto& ex_a : excluded_actions){
                if(ex_a.victim == victim 
                    && ex_a.target == "t" + std::to_string(i) 
                ){
                    excluded = true;
                    break;
                }
            }
            if(i == u || excluded){
                continue;
            }else{
                action a;
                a.victim = victim;
                a.target = target;
                a.assigned_shard = false;
                a.joined_on_shard_key = false;
                a.qm = make_shared<query_mutator>(a.victim, a.find_target_table(s), a.assigned_shard, a.joined_on_shard_key);
                actions.push_back(a);
                weights.push_back(4.0);
            }
        }
    }
    return actions;
}

function<table_type_info(dds)> get_table_type_info_from_dds(string dbms_name){
    if(dbms_name == "citus"){
        return [](dds d){
            table_type_info tti;
            auto it = d.params.find("type");
            if(it != d.params.end()){
                if(it->second == "local" || it->second == "0"){
                    tti.type = 0;
                }else if(it->second == "reference" || it->second == "1"){
                    tti.type = 1;
                }else if(it->second == "distributed" || it->second == "2"){
                    tti.type = 2;
                }
            }
            return tti;
        };
    }
    else if(dbms_name == "shardingsphere"){
        return [](dds d){
            //todo: sharding-method and local on which node
            table_type_info tti;
            auto it = d.params.find("type");
            if(it != d.params.end()){
                if(it->second == "local" || it->second == "0"){
                    tti.type = 0;
                }else if(it->second == "reference" || it->second == "1"|| it->second == "broadcast"){
                    tti.type = 1;
                }else if(it->second == "distributed" || it->second == "2"|| it->second == "sharded"){
                    tti.type = 2;
                }
            }
            return tti;
        };
    }else if(dbms_name == "clickhouse"){
        return [](dds d){
            //todo: sharding-method and local on which node
            table_type_info tti;
            auto it = d.params.find("type");
            if(it != d.params.end()){
                if(it->second == "0"){
                    tti.type = 0;
                }else if(it->second == "1"){
                    tti.type = 1;
                }else if(it->second == "2"){
                    tti.type = 2;
                }
                else if(it->second == "3"){
                    tti.type = 3;
                }
            }
            return tti;
        };
    }else if(dbms_name == "vitess"){
        return [](dds d){
            table_type_info tti;
            auto it = d.params.find("type");
            if(it != d.params.end()){
                if(it->second == "pinned"){
                    tti.type = 0;
                }else if(it->second == "reference"){
                    tti.type = 1;
                }else if(it->second == "distributed"){
                    tti.type = 2;
                }
            }
            return tti;
        };
    }
    else if(dbms_name == "mysql_ndb"){
        return [](dds d){
            table_type_info tti;
            auto it = d.params.find("type");
            if(it != d.params.end()){
                if(it->second == "local"){
                    tti.type = 0;
                }else if(it->second == "reference"){
                    tti.type = 1;
                }else if(it->second == "colocated" || it->second == "distributed"){
                    tti.type = 2;
                }
            }
            return tti;
        };
    }
    else{
        return [](dds d){
            table_type_info tti;
            auto it = d.params.find("type");
            if(it != d.params.end()){
                if(it->second == "local" || it->second == "0"){
                    tti.type = 0;
                }else if(it->second == "reference" || it->second == "1"){
                    tti.type = 1;
                }else if(it->second == "distributed" || it->second == "2"){
                    tti.type = 2;
                }
            }
            return tti;
        };
    }
}

bool save_invalid_roots(
    const std::map<int, std::shared_ptr<invalid_tree_node>>& roots,
    const std::string& path
){
    json J;
    J["format_version"] = INVALID_TREE_FORMAT_VERSION;
    J["roots"] = json::object();

    for (const auto& [qtype, root] : roots) {
        if (!root) continue;
        J["roots"][std::to_string(qtype)] = dump_node(*root);
    }

    //  rename
    std::filesystem::path p(path);
    auto tmp = p;
    tmp += ".tmp";

    std::ofstream ofs(tmp, std::ios::binary);
    if (!ofs) return false;
    ofs << J.dump(2); // pretty dump() 
    ofs.flush();
    ofs.close();

    std::error_code ec;
    std::filesystem::rename(tmp, p, ec);
    if (ec) return false;
    return true;
}

bool load_invalid_roots(
    std::map<int, std::shared_ptr<invalid_tree_node>>& roots,
    const std::string& path
){
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) return false;

    json J; ifs >> J;

    int ver = J.value("format_version", 0);
    if (ver <= 0 || ver > INVALID_TREE_FORMAT_VERSION) {
        // 
        // 
    }

    roots.clear();
    if (!J.contains("roots")) return true;

    for (auto it = J["roots"].begin(); it != J["roots"].end(); ++it) {
        int qtype = std::stoi(it.key());
        roots[qtype] = load_node(it.value());
    }
    return true;
}