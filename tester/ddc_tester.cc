#include "ddc_tester.hh"
#include "../context/context_factory.hh"
ddc_tester::ddc_tester(shared_ptr<Context> ctx)
{
    context = ctx;
}

shared_ptr<prod> ddc_tester::generate_query_ast(bool with_dml){
    while(true){
        try{
            scope initial_scope;
            context -> db_schema -> fill_scope(initial_scope);
            initial_scope.new_stmt();
            std::shared_ptr<prod> query_ast;
            // query_ast = make_shared<merge_stmt>((struct prod *)0, &initial_scope);
            with_dml = false;
            if(with_dml)
            {            
            int i = dx(100);
            if(i == 2 && !(context->info.distributed_db_name == "mysql_ndb")){
                // query_ast = make_shared<common_table_expression>((struct prod *)0, &initial_scope);
                query_ast = make_shared<query_spec>((struct prod *)0, &initial_scope);
            }
            else if(i == 2){
                query_ast = make_shared<common_table_expression>((struct prod *)0, &initial_scope);
                // query_ast = make_shared<query_spec>((struct prod *)0, &initial_scope);
            }
            else if(i == 3){
                query_ast = make_shared<update_stmt>((struct prod *)0, &initial_scope);
            }
            else if(i == 4){
                query_ast = make_shared<delete_stmt>((struct prod *)0, &initial_scope);
            }
            else if(i == 5){
                query_ast = make_shared<insert_select_stmt>((struct prod *)0, &initial_scope);
            }
            else{
                query_ast = make_shared<query_spec>((struct prod *)0, &initial_scope);
            }
            }
            else{
                query_ast = make_shared<query_spec>((struct prod *)0, &initial_scope);
            }

            // int i = dx(3);
            // if(i == 1)query_ast = make_shared<update_stmt>((struct prod *)0, &initial_scope);
            // else if(i == 2){
            //     query_ast = make_shared<delete_stmt>((struct prod *)0, &initial_scope);
            // }
            // else if(i == 3){
            //     query_ast = make_shared<insert_select_stmt>((struct prod *)0, &initial_scope);
            // }
            return query_ast;
        }catch(exception& e){
            continue;
        }
        break;    
    }
    return nullptr;
}

shared_ptr<query> ddc_tester::generate_query(bool with_dml)
{
    return make_shared<query>(generate_query_ast(with_dml));
}

// void ddc_tester::generate_query_pool(int count){
//     for(int i = 0; i < count; i++){
//         auto q = make_shared<query>(generate_query_ast());
//         generated_query_count++;
//         test(*q);
//         if(q -> is_valid){
//             valid_query_pool.push_back(q);
//         }
//         else{
//             invalid_query_pool.push_back(q);
//         }
//     }
// }

bool ddc_tester::test(query &q)
{
    try {
        context -> master_dut = new_dut_setup(context -> info);
        //context -> master_dut -> test(query, &output, NULL, &env_setting_stmts);
        vector<row_output> output;
        context -> master_dut -> test(q.query_str, &(output), NULL, NULL);
        q.result.clear();
        for(auto &r : output){
            q.result.insert(r);
        }
    } catch(exception& e) {
        q.is_valid = false;
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos);
        if(!expected){
            cerr << "unexpected error: " << err << endl;
        }
        failed_query_count++;
        return false;
    }
    q.is_valid = true;
    success_query_count++;
    if(q.result.size() > 0){
        result_not_null_query_count++;
    }
    return true;
}

bool ddc_tester::test_with_query_plan(query &q)
{
    try {
        //context -> master_dut -> test(query, &output, NULL, &env_setting_stmts);
        context -> master_dut = new_dut_setup(context -> info);

        vector<row_output> output1;
        context -> master_dut -> test(q.query_str, &(output1), NULL, NULL);
        q.result.clear();
        for(auto &r : output1){
            q.result.insert(r);
        }
        
        vector<row_output> output;
        if(!dynamic_pointer_cast<query_spec>(q.ast) && context->db_schema->target_dbms == "clickhouse"){
            q.is_valid = true;
            success_query_count++;
            if(q.result.size() > 0){
                result_not_null_query_count++;
            }
            return true; //clickhouse only support EXPLAIN on select query
        }
            
        cout<<"try to test with query plan"<<endl;
        context -> master_dut -> test("EXPLAIN \n" + q.query_str, &(output), NULL, NULL);
        q.query_plan.clear();
        vector<vector<string>> query_plan;
        for(auto &r : output){
            query_plan.push_back(r);
        }
        q.query_plan = context -> master_dut -> get_query_plan(query_plan);
        // for(auto &r : output){
        //     q.query_plan.push_back(r[0]);
        // }
        // cout<<"query plan: \n"<<q.get_query_plan()<<endl;
    } catch(exception& e) {
        q.is_valid = false;
        q.query_plan.clear();
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos);
        if(!expected){
            cerr << "unexpected error: " << err << endl;
        }
        // if(err.find("correlated subqueries are not supported when the FROM clause contains a reference table") != string::npos){
        //     abort();
        // }
        failed_query_count++;
        return false;
    }
    q.is_valid = true;
    success_query_count++;
    if(q.result.size() > 0){
        result_not_null_query_count++;
    }
    return true;
}