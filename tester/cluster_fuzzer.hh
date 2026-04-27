#ifndef CLUSTER_FUZZ_PROCESS_HH
#define CLUSTER_FUZZ_PROCESS_HH
#include "../dbms_info.hh" // for dbms_info
#include "../context/context.hh"
#include <vector>
#include <memory>

struct cluster_action : prod{
    cluster_action(prod *parent) : prod(parent){}

    virtual void out(std::ostream &out) override {};
    virtual bool run(Context& ctx) {};
    virtual shared_ptr<cluster_action> clone() = 0;
};

struct cluster_fuzzer{
    vector<shared_ptr<cluster_action>> actions;
    shared_ptr<Context> base_context;
    string savefilepath;

    void add_model_action(shared_ptr<cluster_action> action){
        actions.push_back(action);
    }
    
    void clear_actions(){
        actions.clear();
    }

    cluster_fuzzer(shared_ptr<Context> base_context, string savefilepath = "") : base_context(base_context), savefilepath(savefilepath){}

    virtual void slightly_fuzz(int mutate_times=-1);
    virtual void reset_init() {};
};

#endif