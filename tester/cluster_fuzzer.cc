#include "cluster_fuzzer.hh"
#include "../random.hh"


void cluster_fuzzer::slightly_fuzz(int mutate_times){
    if(mutate_times == -1) mutate_times = d6();
    for(int i = 0; i < mutate_times; i++){
        vector<shared_ptr<cluster_action>> candidate_actions;
        for(auto action:actions){
            candidate_actions.push_back(action -> clone());
        }
        auto action = random_pick(candidate_actions);
        action -> run(*base_context);
    }
}


// void cluster_fuzzer_base::fuzz(bool should_mutate){
//     reset_init();
//     cerr<<"start fuzz: " << name <<" should_mutate: " << should_mutate <<endl;
//     mutator->add_template_to_seed_pool();
//     //select a seed
//     mutator->get_new_seed();
//     //mutate
//     if(should_mutate){
//         auto victim_actions = mutate_and_run(*(mutator->victim_actions));
//     }
//     //execute actions
//     else try{
//         for(const auto& action : *(mutator->victim_actions)){
//             action -> run(*base_context);
//         }
//     }catch(exception& e){
//         throw;
//     }
//     //if succeed, check and add it to seed
//     if(should_mutate){
//         mutator->add_victim_to_seed_pool();
//     }
// }

// action_sequence cluster_fuzzer_base::mutate_and_run_template(action_sequence& victim){
//     action_sequence mutated_actions = victim.clone();
//     size_t index_to_mutate = dx(mutated_actions.size()) - 1;
//     cout<<"mutated_actions size: "<<mutated_actions.size()<<", index_to_mutate: "<<index_to_mutate<<endl;
//     //first, run actions until index_to_mutate
//     for(auto i = mutated_actions.begin(); i < mutated_actions.begin() + index_to_mutate; i++){
//         (*i) -> run(*base_context);
//     }
//     vector<int> mutate_index_list = {1,2,3,4};
//     // Use try-catch to handle exceptions, if exception, loop at most 10 times
//     for(int j = 0; j < 20; j++){ 
//         int i = random_pick(mutate_index_list);
//         if(mutated_actions.size() < 15 && d6() < 5) i = 2;
//         cout <<"round: "<< j << ", mutate_and_run: " << i << endl;
//         try {
//             if(i == 1){
//                 try{
//                     mutated_actions[index_to_mutate] -> random_fill(*base_context);
//                     //mutated_actions.actions[index_to_mutate] -> out(cout);
//                 }
//                 catch(exception& e){
//                     //remove the first element (0) of mutate_index_list
//                     mutate_index_list.erase(mutate_index_list.begin());
//                     continue;
//                 }
//             } else if(i == 2){
//                 auto action = mutator->factory -> random_pick();
//                 action -> random_fill(*base_context);
//                 //action -> out(cout);
//                 mutated_actions.insert(mutated_actions.begin() + index_to_mutate, std::move(action));
//             } else if(i == 3){
//                 mutated_actions.erase(mutated_actions.begin() + index_to_mutate);
//                 index_to_mutate--;
//             } else {
//                 auto action = mutator->factory -> random_pick();
//                 action -> random_fill(*base_context);
//                 mutated_actions.insert(mutated_actions.begin() + index_to_mutate, std::move(action));
//                 //mutated_actions.actions[index_to_mutate] -> out(cout);
//             }

//             if(i != 3) mutated_actions[index_to_mutate] -> run(*base_context);
//             break;
//         } catch (const std::exception& e) {
//             string err = e.what();
//             cerr << "Exception during mutation: " << err << endl;
//             if (err.find("expected error") == string::npos) {
//                 // If this is not an expected error, rethrow it
//                 throw;
//             }
//             // If it's an expected error, continue with next action
//             continue;
//         }
//     }
    
//     //finally, run the rest of the actions
//     for(auto i = mutated_actions.begin() + index_to_mutate + 1; i != mutated_actions.end(); i++){
//         (*i) -> run(*base_context);
//     }
//     return mutated_actions;
// }

// void cluster_fuzzer::run(){
//     for(int i = 0; i < 80; i++){
//         ruin();
//         cout<<"cluster_fuzzer round " << i << endl;
//         try{
//             for(auto fuzzer:fuzzers){
//                 fuzzer -> fuzz(true);
//             }
//             return;
//         }catch(exception& e){
//             //check error
//             string err = e.what();
//             bool expected = (err.find("expected error") != string::npos);
//             if(!expected) {
//                 reproduce_and_minimize();
//                 throw;
//             }
//             cout<<"cluster_fuzzer failed, retest"<<endl;
//         }
//     }
//     cout << "cluster_fuzzer failed, retest by init_template" << endl;
//     run_default();
// }

// void cluster_fuzzer::run_default(){
//     try{
//         for(auto fuzzer:fuzzers){
//             fuzzer -> fuzz(false);
//         }
//     }catch(exception& e){
//         cout<<"init_template failed, please check the func is correct"<<endl;
//         abort();
//     }
// }

// void cluster_fuzzer::reproduce_and_minimize(){
//     //should be cluster_actions.sql
//     // TODO: maybe useless
//     ofstream ofile("citus/saved/error.sql", ios::out | ios::trunc);
//     for(auto fuzzer:fuzzers){
//         for(const auto& action : *(fuzzer -> mutator->victim_actions)){
//             action -> out(ofile);
//         }
//     }
//     ofile.close();
// }
