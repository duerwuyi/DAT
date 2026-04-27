#ifndef EXTRACT_FEATURE_HH
#define EXTRACT_FEATURE_HH

#include "action.hh"
#include "../tester/ddc_tester.hh"
#include "nlohmann/json.hpp"

using nlohmann::json;

struct ast_relation{
    int id;
    // struct ast_relation *parent;
    // int parent_id;
    struct prod* prod_ptr;
    //used for terminal node
    bool is_terminal = false;
    // table * terminal_table;
    string terminal_table_name;
    string reference_name;
    named_relation* alias_ptr = nullptr;
    //used for dml
    table * modify = nullptr;
    // //used for relational operators
    // pair<int, int> made_from;
    // string operator_type; // join , union , intersect , except
    // //used for aggregation
    // bool aggregated = false;
    // //used for tracking relationships
    // vector<int> joined_with; // vector<int> join_leaf_terminals;
    // vector<int> set_oper_with;
    // vector<int> used_by;

    // relational operators
    map<string, vector<int>> operator_relations;

    //used for action impact
    bool assigned_shard = false;
    bool joined_on_shard_key = false;

    bool victim_assigned_shard = false;
    bool victim_joined_on_shard_key = false;

    void print(){
        cout << "AST Relation ID: " << id;
        if(is_terminal){
            cout << ", Terminal Table: " << terminal_table_name;
        }
        if(operator_relations.size() > 0)
            cout << ", Operator Relations: " ;
        else cout << endl;
        for(auto& op_rel : operator_relations){
            cout << "  " << op_rel.first << ": ";
            for(auto& rel_id : op_rel.second){
                cout << rel_id << " ";
            }
        }
        cout << endl;
    }
};

struct table_type_info{
    int type; // 0-local table, 1-reference table, 2-distributed table
    //used for action impact
    bool assigned_shard = false;
    bool joined_on_shard_key = false;

    bool operator==(const table_type_info& other) const {
        return (type == other.type) &&
               (assigned_shard == other.assigned_shard) &&
               (joined_on_shard_key == other.joined_on_shard_key);
    }

    bool operator<(const table_type_info& other) const {
        if (type < other.type) return true;
        if (type > other.type) return false;

        if(joined_on_shard_key < other.joined_on_shard_key) return true;
        if(other.joined_on_shard_key < joined_on_shard_key) return false;

        return assigned_shard < other.assigned_shard;
    }
};

int table_type_info_to_int(table_type_info t);

table_type_info int_to_table_type_info(int v);

inline void to_json(nlohmann::json& j, const table_type_info& t) {
    j = nlohmann::json{
        {"type", t.type},
        {"assigned_shard", t.assigned_shard},
        {"joined_on_shard_key", t.joined_on_shard_key}
    };
}
inline void from_json(const nlohmann::json& j, table_type_info& t) {
    t.type = j.at("type").get<int>();
    t.assigned_shard = j.value("assigned_shard", false);
    t.joined_on_shard_key = j.value("joined_on_shard_key", false);
}

struct relation_chain{
    table_type_info from;
    table_type_info to;
    vector<string> operator_types;

    bool victim_assigned_shard = false;
    bool victim_joined_on_shard_key = false;

    bool operator==(const relation_chain& other) const {
        return (from == other.from) &&
               (to == other.to) &&
               (operator_types == other.operator_types);
    }

    bool operator<(const relation_chain& other) const {
        if (from < other.from) return true;
        if (other.from < from) return false;

        if (to < other.to) return true;
        if (other.to < to) return false;

        return operator_types < other.operator_types;
    }

    void print(){
        cout << "From type: " << from.type<<", shard:"<<from.assigned_shard<<", join:"<<from.joined_on_shard_key << ", Operators: ";
        for(auto& op : operator_types){
            cout << op << " ";
        }
        cout << ", To type: " << to.type<<", shard:"<<to.assigned_shard<<", join:"<<to.joined_on_shard_key;
        cout << endl;
    }
};


struct table_type_info_hash {
    std::size_t operator()(const table_type_info& t) const {
        std::size_t seed = 0;

        auto hash_combine = [](std::size_t& seed, std::size_t value) {
            seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        };

        hash_combine(seed, std::hash<int>{}(t.type));
        hash_combine(seed, std::hash<bool>{}(t.assigned_shard));
        hash_combine(seed, std::hash<bool>{}(t.joined_on_shard_key));

        return seed;
    }
};

struct relation_chain_hash {
    std::size_t operator()(const relation_chain& rc) const {
        std::size_t seed = 0;

        auto hash_combine = [](std::size_t& seed, std::size_t value) {
            seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        };

        table_type_info_hash table_hasher;

        hash_combine(seed, table_hasher(rc.from));
        hash_combine(seed, table_hasher(rc.to));

        hash_combine(seed, std::hash<std::size_t>{}(rc.operator_types.size()));
        for (const auto& op : rc.operator_types) {
            hash_combine(seed, std::hash<std::string>{}(op));
        }

        return seed;
    }
};

namespace std {
template <>
struct hash<relation_chain> {
    std::size_t operator()(const relation_chain& rc) const {
        std::size_t seed = 0;

        auto hash_combine = [](std::size_t& seed, std::size_t value) {
            seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        };

        hash_combine(seed, std::hash<int>{}(rc.from.type));
        hash_combine(seed, std::hash<bool>{}(rc.from.assigned_shard));
        hash_combine(seed, std::hash<bool>{}(rc.from.joined_on_shard_key));

        hash_combine(seed, std::hash<int>{}(rc.to.type));
        hash_combine(seed, std::hash<bool>{}(rc.to.assigned_shard));
        hash_combine(seed, std::hash<bool>{}(rc.to.joined_on_shard_key));

        hash_combine(seed, std::hash<std::size_t>{}(rc.operator_types.size()));
        for (const auto& op : rc.operator_types) {
            hash_combine(seed, std::hash<std::string>{}(op));
        }

        return seed;
    }
};
}

vector<relation_chain> find_relation_chain(string source_table_name,
    map<int, ast_relation>& ast_relations,
    multimap<string, int>& table_name_to_ast_relation_id,
    map<string, dds> table_dds_info,
    function<table_type_info(dds)> get_table_type_info
);

struct invalid_tree_node{
    vector<shared_ptr<invalid_tree_node>> children;
    bool is_start;
    bool is_terminal;
    table_type_info table_info;
    string operator_type;
    int occurences = 0;
    int ivalid = 0;
    map<int,int> total_plan_each_victim;
    map<int,int> unique_plan_each_victim;

    bool operator==(const invalid_tree_node& other) const {
        return (is_terminal == other.is_terminal) &&
               (table_info.type == other.table_info.type) &&
               (operator_type == other.operator_type);
    }

    int vote_for(invalid_tree_node* parent,int occ_threshold = 10, double inval_threshold = 0.89){
        double max_unique_rate = 0.0;
        int res = -1;
        for(auto& p: total_plan_each_victim){
            int victim = p.first;
            
            // examine if enough validity
            table_type_info t = int_to_table_type_info(victim);
            bool enough_validity = true;
            for(auto& child: parent->children){
                if(child->is_terminal && child->table_info == t){
                    const double invalidity = child->occurences ? (double)child->ivalid / child->occurences : 0.0;
                    if (child->occurences > occ_threshold && invalidity > inval_threshold) {
                        // not enough invalidity
                        enough_validity = false;
                        break;
                    }
                }
            }
            if (!enough_validity) continue;
            
            int total = p.second;
            int unique = unique_plan_each_victim[victim];
            double unique_rate = (total == 0) ? 0.0 : (double)unique / total;
            if(unique_rate > max_unique_rate || max_unique_rate == 0.0){
                max_unique_rate = unique_rate;
                res = victim;
            }
        }
        cout<<"vote result: "<<res<<", unique rate: "<< max_unique_rate << endl;
        return res;
    }
};

static nlohmann::json dump_node(const invalid_tree_node& n) {
    using nlohmann::json;
    json J;
    J["is_start"]      = n.is_start;
    J["is_terminal"]   = n.is_terminal;
    J["table_info"]    = n.table_info;      //  to_json(table_type_info)
    J["operator_type"] = n.operator_type;
    J["occurences"]    = n.occurences;
    J["ivalid"]        = n.ivalid;

    {
        json obj = json::object();
        for (const auto& [victim, total] : n.total_plan_each_victim) {
            obj[std::to_string(victim)] = total;
        }
        J["total_plan_each_victim"] = std::move(obj);
    }
    {
        json obj = json::object();
        for (const auto& [victim, unique] : n.unique_plan_each_victim) {
            obj[std::to_string(victim)] = unique;
        }
        J["unique_plan_each_victim"] = std::move(obj);
    }
    // children
    J["children"] = json::array();
    auto& arr = J["children"];  // json&
    arr.get_ref<json::array_t&>().reserve(n.children.size());
    for (const auto& ch : n.children) {
        arr.push_back(dump_node(*ch));      //  dump
    }
    return J;
}

static std::shared_ptr<invalid_tree_node> load_node(const nlohmann::json& J) {
    using nlohmann::json;
    auto n = std::make_shared<invalid_tree_node>();
    n->is_start      = J.value("is_start",    false);
    n->is_terminal   = J.value("is_terminal", false);
    if (J.contains("table_info"))    n->table_info   = J.at("table_info").get<table_type_info>();
    if (J.contains("operator_type")) n->operator_type = J.at("operator_type").get<std::string>();
    n->occurences    = J.value("occurences", 0u);
    n->ivalid        = J.value("ivalid",     0u);

    if (J.contains("total_plan_each_victim") && J["total_plan_each_victim"].is_object()) {
        for (const auto& [k, v] : J["total_plan_each_victim"].items()) {
            int victim = std::stoi(k);
            int total  = v.get<int>();
            n->total_plan_each_victim[victim] = total;
        }
    }
    if (J.contains("unique_plan_each_victim") && J["unique_plan_each_victim"].is_object()) {
        for (const auto& [k, v] : J["unique_plan_each_victim"].items()) {
            int victim = std::stoi(k);
            int unique = v.get<int>();
            n->unique_plan_each_victim[victim] = unique;
        }
    }

    if (auto it = J.find("children"); it != J.end() && it->is_array()) {
        n->children.reserve(it->size());    // 
        for (const auto& jc : *it) {
            n->children.push_back(load_node(jc));
        }
    }
    return n;
}



struct table_usage_in_from_clause {
    string table_name;
    int from_clause_id;
    int depth;
    table_usage_in_from_clause(string table_name, int from_clause_id, int depth) : table_name(table_name), from_clause_id(from_clause_id), depth(depth) {}
};

struct query_feature{
    //vector<int> colocated_id;
    vector<vector<int>> usages;
    string query_plan;
    bool is_valid;
};

inline void to_json(nlohmann::json& j, const query_feature& qf) {
    j = nlohmann::json{
        {"total_groups", total_groups},
        {"usages", qf.usages},
        {"query_plan", qf.query_plan},
        {"is_valid", qf.is_valid}
    };
}

int find_query_type(struct prod *p);

// table_type_info electing_target_in_tree(
//     map<int, shared_ptr<invalid_tree_node>>& invalid_tree_root,
//     prod * p,
//     vector<relation_chain> relation_chains,
//     table_type_info victim
// );

bool search_invalidity_in_tree(
    map<int, shared_ptr<invalid_tree_node>>& invalid_tree_root,
    prod * p,
    vector<relation_chain> relation_chains,
    int occ_threshold = 10,
    double inval_threshold = 0.89 // if more than threshold of relation chains are invalid, then we consider the query is invalid
);


bool add_relation_chain_to_tree(
    std::map<int, std::shared_ptr<invalid_tree_node>>& roots,
    prod* p,
    std::vector<relation_chain>& relation_chains,
    bool is_valid,
    table_type_info victim,
    bool is_unique_query_plan,
    int occ_threshold = 10,
    double inval_threshold = 0.89 // if more than threshold of relation chains are invalid, then we consider the query is invalid
);

function<table_type_info(dds)> get_table_type_info_from_dds(string dbms_name);

struct extracter : prod_visitor {
    //vector<table_usage_in_from_clause> usages;
    vector<string> used_table_names;
    map<int, ast_relation> ast_relations;//#0 is root
    multimap<string,int> table_id_to_ast_relation_id;
    int max_from_clause_id = 0;
    string first_table_name = "";

    virtual void visit(struct prod *p);
    query_feature extract_feature(shared_ptr<schema> s, query &q);

    vector<int> used_table();
    set<int> used_table_sets();
    vector<action> avalilable_actions(shared_ptr<schema> s, vector<double>& weights, vector<action> excluded_actions = {}, int victim = -1);
    vector<action> avalilable_actions2(shared_ptr<schema> s,vector<string> candidate_targets,
         vector<action> excluded_actions = {},bool assigned_shard = false, bool joined_on_shard_key = false);
    vector<action> avalilable_actions3(shared_ptr<schema> s, vector<double>& weights, vector<action> excluded_actions = {}, int victim = -1);
};

constexpr int INVALID_TREE_FORMAT_VERSION = 1;

bool save_invalid_roots(
    const std::map<int, std::shared_ptr<invalid_tree_node>>& roots,
    const std::string& path
);

bool load_invalid_roots(
    std::map<int, std::shared_ptr<invalid_tree_node>>& roots,
    const std::string& path
);

#endif