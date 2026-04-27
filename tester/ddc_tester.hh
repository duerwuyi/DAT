#ifndef DDC_TESTER_HH
#define DDC_TESTER_HH

#include "../dbms_info.hh"
#include "../schema.hh"
#include "../prod.hh"
#include "../grammar.hh"
#include "../dut.hh"
#include "../context/context.hh"

struct query{
    string set_stmt;
    shared_ptr<prod> ast;
    string query_str;
    multiset<row_output> result;
    vector<string> query_plan;
    bool is_valid;
    int valid_action_count = 0;
    int unique_action_count = 0;

    query(shared_ptr<prod> ast){
        this -> ast = ast;
        if(dynamic_pointer_cast<query_spec>(ast)){
            auto select_stmt = dynamic_pointer_cast<query_spec>(ast);
            select_stmt -> hint = "";
        }
        ostringstream s;
        ast -> out(s);
        query_str = s.str();
        s.clear();
        is_valid = false;
    }

    void refresh(string ddbms = ""){
        ostringstream s;
        if(ddbms == "tidb" && d6()>4 && dynamic_pointer_cast<query_spec>(ast)){
            auto select_stmt = dynamic_pointer_cast<query_spec>(ast);
            select_stmt -> hint = "/*+ SET_VAR(tidb_allow_mpp=1) SET_VAR(tidb_enforce_mpp=1) */ \n";
        }else if(dynamic_pointer_cast<query_spec>(ast)){
            auto select_stmt = dynamic_pointer_cast<query_spec>(ast);
            select_stmt -> hint = "";
        }
        ast -> out(s);

        query_str = s.str();
        s.clear();
        result.clear();
        is_valid = false;
    }

    string get_query_plan(){
        if(query_plan.empty()){
            return "";
        }
        ostringstream s;
        for(auto &r : query_plan){
            s << r << "\n";
        }
        return s.str();
    }
};

struct ddc_tester{
    shared_ptr<Context> context;

    int generated_query_count = 0;
    int result_not_null_query_count = 0;
    int success_query_count = 0;
    int failed_query_count = 0;

    ddc_tester(shared_ptr<Context> ctx);

    bool test(query &q);
    bool test_with_query_plan(query &q);
    shared_ptr<prod> generate_query_ast(bool with_dml = true);
    shared_ptr<query> generate_query(bool with_dml = true);
};

#endif