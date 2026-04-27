// Distranger differential checker
#include "checker/ddc_checker.hh"
#include "../globals.h"
#include "../prod.hh"
#include <regex>
#include <boost/regex.hpp>
#include <unordered_set>
#include <fstream>
#include <sstream>
using boost::regex;
using boost::smatch;
using boost::regex_match;

static inline void trim_inplace(string& s) {
    auto not_space = [](int ch){ return !std::isspace(ch); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
    s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
}

double file_size_mib(const std::filesystem::path& p) {
    namespace fs = std::filesystem;

    if (!fs::exists(p)) {
        throw std::runtime_error("File not found: " + p.string());
    }
    if (!fs::is_regular_file(p)) {
        throw std::runtime_error("Not a regular file: " + p.string());
    }

    std::uintmax_t bytes = fs::file_size(p);
    return static_cast<double>(bytes) / (1024.0 * 1024.0); // MiB
}

double file_size_mb_decimal(const std::filesystem::path& p) {
    namespace fs = std::filesystem;

    if (!fs::exists(p)) {
        throw std::runtime_error("File not found: " + p.string());
    }
    if (!fs::is_regular_file(p)) {
        throw std::runtime_error("Not a regular file: " + p.string());
    }

    std::uintmax_t bytes = fs::file_size(p);
    return static_cast<double>(bytes) / 1'000'000.0; // MB (decimal)
}

void ddc_checker::run_ddc() {
    //max_level = min(100, max_level);
    max_level = 20;
    if(dynamic_pointer_cast<vitess_context>(tester->context)) max_level = 10; //bug #19532
    cout << "max_level for this run: " << max_level << endl;
    // executed_query_count = 0;
    // invalid_query_count = 0;
    // valid_query_count = 0;

    fuzzer->reset_init();
    // tester->context->db_schema = generator->init_db();
    this->generator = make_shared<diff_db_mutator>(tester->context, compared);
    tester->context->db_schema = generator->init_db();

    for (int i = 0; i < total_query_count; i++) {
        if (shutdown_requested)
            return;
    // for (; executed_query_count < 1000;) {
        //open file to log
        // ofstream log_file("ddc_checker.log", ios::app);
        // log_file << "========================" << endl;
        //generate a query(if its from compared_tester, we can run diff; or else just run it to get crash bug)
        //because distributed dbms may have unique feature or function.
        bool dml = false;
        auto q = tester->generate_query(dml); //generated from distri dbms
        auto q1 = make_shared<query>(q->ast);//used for distributed dbms
        // log_file << "Generated Query: \n" << q->query_str << endl;
        compared_tester->test(*q);
        executed_query_count++;
        multiset<row_output> output1;
        if(q->is_valid){
            // log_file<<"single query plan:\n"<< q->get_query_plan() << endl;
            output1 = q->result;
        }else{
            invalid_query_count++;
            // log_file<<"single query is invalid\n";
            cout<<"single query is invalid\n";
            continue;
        }
        q->refresh();
        tester->test(*q);
        if(q->is_valid){
            valid_query_count++;
            log(q->query_str);
            // log_file<<"distributed query plan:\n"<< q->get_query_plan() << endl;
            multiset<row_output> output2 = q->result;
            //check result
            if(check_result(output1, output2, q->query_str)){
                if(!dynamic_cast<query_spec*>(q->ast.get()) && !output1.empty()){
                    generator -> elimitate_inconsistent_data(fuzzer);
                }
            }
        }else{
            invalid_query_count++;
            // log_file<<"distributed query is invalid\n";
            cout<<"distributed query is invalid\n";
            if(!dynamic_cast<query_spec*>(q->ast.get()) && !output1.empty()){
                generator -> elimitate_inconsistent_data(fuzzer);
            }
            continue;
        }

        select_and_run_action(q, actions_per_query);
        if (shutdown_requested)
            return;
        
    }
    print();
    save_invalid_roots(invalid_tree, "invalid_context_trees.json");
    try {
        double sz = file_size_mib("invalid_context_trees.json");
        std::cout << std::fixed << std::setprecision(2) << sz << " MiB\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
    }
}

void ddc_checker::select_and_run_action(shared_ptr<query> q, int round) {
    vector<action> excluded_actions;
    action a;
    for(int i = 0;i<round;i++){
        if (shutdown_requested)
            return;
        bool valid_before_action = false;
        vector<relation_chain> relation_chains;
        int revert  = 0;
        //int victim = 0;
        while(!valid_before_action && revert < 5){
            extracter e;
            e.extract_feature(tester->context->db_schema, *q);
            vector<int> used_tables = e.used_table();
            if(used_tables.size() == 0) {cout<<"no avalible used_tables"<<endl;goto end_actions;}
            vector<double> weights;
            auto actions = e.avalilable_actions(tester->context->db_schema, weights, excluded_actions);
            if(actions.size() == 0) {cout<<"no avalible actions"<<endl;goto end_actions;}
            a = weighted_random_pick(actions, weights);
            excluded_actions.push_back(a);
            if(!a.has_resolved_mutator()){
                cout << "skip action with unresolved mutator";
                if(a.qm){
                    cout << ": victim=" << a.qm->victim;
                }
                cout << endl;
                revert++;
                continue;
            }
            cout<< "action: victim: "<< a.qm -> victim<<" mutator: "<<a.qm->substitute->ident()
                <<" shard: "<<a.qm->assigned_shard<<", colocated join "<<a.qm->joined_on_shard_key<<endl;
            if(a.target == "") goto end_actions;
            q -> ast -> accept(a.qm.get());
            q -> refresh(tester->context->info.distributed_db_name);
            // }
            e.extract_feature(tester->context->db_schema, *q);
            relation_chains = find_relation_chain(
                a.target,
                e.ast_relations,
                e.table_id_to_ast_relation_id,
                tester->context->db_schema->ddss,
                get_table_type_info_from_dds(tester->context->info.distributed_db_name)
            );

            for(auto& rc : relation_chains){
                rc.print();
            }

            valid_before_action = search_invalidity_in_tree(
                invalid_tree,
                q -> ast.get(),
                relation_chains
            );
            if(!valid_before_action){
                // cout << "revert action due to invalidity detected in tree" << endl;
                // try {
                //     auto a_reverse = a.get_reverse_action(tester->context->db_schema);
                //     cout << "revert action: mutator: " << a_reverse.qm->substitute->ident() << endl;
                //     q -> ast -> accept(a_reverse.qm.get());
                //     q -> refresh();
                //     revert++;
                // } catch(const exception& ex) {
                //     cout << "skip revert due to unresolved reverse action: " << ex.what() << endl;
                //     goto end_actions;
                // }
                break;
            }
        }
        if(!valid_before_action){
            cout << "cannot find valid action, end actions for this query" << endl;
            goto end_actions;
        }
        // if(relation_chains.size() == 0){
        //     cout << "no relation chain after mutation, skip invalid-tree check" << endl;
        //     goto end_actions;
        // }
        //test with oracle
        tester->test(*q);
        //collect metrics
        executed_query_count++;
        //update invalid tree
        add_relation_chain_to_tree(
            invalid_tree,
            q -> ast.get(),
            relation_chains,
            q->is_valid,
            get_table_type_info_from_dds(tester->context->info.distributed_db_name)(tester->context->db_schema->ddss.at(a.target)),
            false
        );
        if(q->is_valid){
            //oracle check
            multiset<row_output> output2 = q->result;
            auto q1 = make_shared<query>(q->ast);
            compared_tester->test(*q1);
            multiset<row_output> output1 = q1->result;
            if(check_result(output1, output2, q->query_str)){
                //bug
                return;
            }
            if(!dynamic_cast<query_spec*>(q->ast.get()) && !output1.empty() &&  !output2.empty()){
                generator -> elimitate_inconsistent_data(fuzzer);
            }

            q->valid_action_count++;
            valid_query_count++;
            log(q->query_str);
        }else{
            cout << "revert action due to action error" << endl;
            try {
                auto a_reverse = a.get_reverse_action(tester->context->db_schema);
                cout << "revert action: mutator: " << a_reverse.qm->substitute->ident() << endl;
                q -> ast -> accept(a_reverse.qm.get());
                q -> refresh();
            } catch(const exception& ex) {
                cout << "skip revert due to unresolved reverse action: " << ex.what() << endl;
                goto end_actions;
            }
            invalid_query_count++;
        }
    }
end_actions:
    return;
}

void ddc_checker::print() {
    ostringstream out;
    out << "mode: ddc" << endl;
    out << "elapsed_seconds: " << elapsed_runtime_seconds() << endl;
    out << "elapsed_hms: " << elapsed_runtime_hms() << endl;
    out << "invalid_query_count: " << invalid_query_count << endl;
    out << "valid_query_count: " << valid_query_count << endl;
    out << "executed_query_count: " << executed_query_count << endl;
    out << "validity: " << valid_query_count * 1.0 / executed_query_count << endl;

    string text = out.str();
    cout << text;
    ofstream summary_log("chain_summary_ddc.log", ios::app);
    summary_log << "--- print ---" << endl;
    summary_log << text << endl;
}

bool ddc_checker::check_result(multiset<row_output> a, multiset<row_output> b,string qstr){
    if(!check(a, b)){
        save_query(".", "unexpected_" + to_string(bug) +".sql", qstr);
        save_backup_file(".", tester->context->info);
        bug++;
        abort();
        // return true;
        //if logic bug, abort
    }
    else return false;
}
