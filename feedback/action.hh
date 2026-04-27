#ifndef ACTION_HH
#define ACTION_HH

#include "../prod.hh"
#include "../context/context.hh"
#include "nlohmann/json.hpp"

struct query_mutator : prod_visitor {
    bool assigned_shard = false;
    bool joined_on_shard_key = false;

    bool victim_assigned_shard = false;
    bool victim_joined_on_shard_key = false;
    string victim;
    table* substitute;
    
    virtual void visit(struct prod *p);

    query_mutator(){}

    query_mutator(string victim, table* substitute,
        bool assigned_shard = false,
        bool joined_on_shard_key = false
    ){
        this->assigned_shard = assigned_shard;
        this->joined_on_shard_key = joined_on_shard_key;
        this->victim = victim;
        this->substitute = substitute;
    }
    //change with dkey
};

vector<dds> get_dds_list(shared_ptr<schema> s);

//change dds
struct action{
    string victim;
    string target;
    bool assigned_shard = false;
    bool joined_on_shard_key = false;
    bool victim_assigned_shard = false; //for reverse action
    bool victim_joined_on_shard_key = false; //for reverse action
    shared_ptr<query_mutator> qm;
    int get_target_group_id(){
        if(target == ""){
            return -1;
        }
        if(target[0] == 't'){
            return std::stoi(target.substr(1)) % total_groups;
        }
        return -1;
    }

    table* find_target_table(shared_ptr<schema> s){
        table* target_table = nullptr;
        for(auto& t : s->tables) {          //   &
            if (t.name == target) {
                target_table = &t;            //  
                break;
            }
        }
        return target_table;
    }

    bool has_resolved_mutator() const {
        return qm && qm->substitute;
    }

    action get_reverse_action(shared_ptr<schema> s){
        if(victim == "-1") {
            throw std::runtime_error("cannot get reverse action when victim is -1");
        }
        action reverse_action;
        reverse_action.victim = target;
        reverse_action.target = victim;

        reverse_action.assigned_shard = victim_assigned_shard;
        reverse_action.joined_on_shard_key = victim_joined_on_shard_key;

        reverse_action.victim_assigned_shard = assigned_shard;
        reverse_action.victim_joined_on_shard_key = joined_on_shard_key;

        auto* reverse_substitute = reverse_action.find_target_table(s);
        if (!reverse_substitute) {
            throw std::runtime_error(
                "cannot resolve reverse action substitute: victim=" + reverse_action.victim +
                ", target=" + reverse_action.target);
        }
        reverse_action.qm = make_shared<query_mutator>(reverse_action.victim, reverse_substitute);
        return reverse_action;
    }
};


/*
params:
victim: table name
target: table name
todo: instruments: need special handling?
*/
action get_action_from_server(shared_ptr<schema> s, std::unordered_map<std::string, std::string>& params);

//only used when init all tables
struct create_dds : prod{
    shared_ptr<table> victim;
    shared_ptr<dds> victim_dds;
    create_dds(shared_ptr<table> v, prod* parent = nullptr)
        : prod(parent){this->victim = v;}

    virtual bool run(Context& ctx) {};
    virtual void out(std::ostream &out) override {};
};

struct distributor{
    vector<shared_ptr<action>> actions;
    shared_ptr<Context> ctx;
    distributor(shared_ptr<Context> context): ctx(context){}
    virtual string distribute_one(shared_ptr<prod> gen, int* affect_num, int i = -1) {};
    virtual void clear_record_file() = 0;
};

// struct dds{
//     /*
//     params:
//     victim: table name
//     type: table type
//     dkey: dkey name(string)
//     colocate: colocate table name(string) (citus only)
//     shard_count: shard count(int)
//     */
//    std::unordered_map<std::string, std::string> params;
// };

// inline void to_json(nlohmann::json& j, const dds& d) {
//     j = nlohmann::json{
//         {"params", d.params}
//     };
// }

struct feedback{
    /*
    params:
    bandit_new_query_plan_found: number of new query plan found
    */
    std::unordered_map<std::string, std::string> params;
    double reward = 0.0;   // 
    bool   stop   = false; // 
};

inline void to_json(nlohmann::json& j, const feedback& f) {
    j["reward"] = f.reward;
    j["stop"]   = f.stop;
    for(auto it = f.params.begin(); it != f.params.end(); it++) {
        j[it->first] = it->second;
    }
}

#endif
