#ifndef DDC_CHECKER_HH
#define DDC_CHECKER_HH

#include "checker/common_fuzz_checker.hh"
#include <unordered_set>

struct ddc_checker : common_fuzz_checker {
    shared_ptr<ddc_tester> compared_tester;

    ddc_checker(shared_ptr<Context> context, shared_ptr<Context> compared = nullptr,
         bool need_schema = true, bool should_change_schema = false, int total_query_count = 10000)
         : common_fuzz_checker(context, compared, need_schema, should_change_schema, total_query_count){
            compared_tester = make_shared<ddc_tester>(compared);
            //init static variable by single dbms, used for generating query
            compared -> db_schema = get_new_schema(compared -> info);
            schema::random_init = false;
            //create a file as log
            ofstream log_file("ddc_checker.log", ios::out);
            log_file.close();
            // max_level = 3; // reset max_level for each run
            bool i = load_invalid_roots(invalid_tree, "invalid_context_trees.json");
            if(i){
                cout << "load invalid tree from file success" << endl;
            }else{
                cout << "load invalid tree from file failed, start with empty tree" << endl;    
                for(int i = 0;i<5;i++)
                    invalid_tree[i] = make_shared<invalid_tree_node>();
            }
         }
    
    int invalid_query_count = 0;
    int valid_query_count = 0;
    int executed_query_count = 0;

    map<int, shared_ptr<invalid_tree_node>> invalid_tree;

    int actions_per_query = 9;

    void run_ddc();

    void select_and_run_action(shared_ptr<query> q,int round = 10);

    int bug = 0;

    bool check_result(multiset<row_output> a, multiset<row_output> b,string qstr);
    
    void print();
};

#endif
