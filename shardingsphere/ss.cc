#include "ss.hh"
#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

static bool is_expected_error_in_distributed_query(string error){
    if (error.find("Duplicate storage unit") != string::npos
        )
    return true;
}

static bool duplicated_storage_unit(string error){
    if (error.find("Duplicate storage unit") != string::npos
        )
        return true;
    else
        return false;
}

void ss_dut::reset_to_backup(void){
}

void ss_dut::test(const string &stmt, 
                    vector<vector<string>>* output, 
                    int* affected_row_num,
                    vector<string>* env_setting_stmts){
    if(stmt.find("EXPLAIN") == string::npos &&
       stmt.find("SHOW") == string::npos
    ){
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        std::tm now_tm = *std::localtime(&now_time_t);
        cout<<"ss test:"<< std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S")
                << '.' << std::setfill('0') << std::setw(5) << now_ms.count() << std::endl;
    }
    try{
        dut -> test(stmt,output,affected_row_num,env_setting_stmts);
    }catch(exception& e){
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos);
        if(expected) throw;
        if(duplicated_storage_unit(err)){
            return;
        }
        if(is_expected_error_in_distributed_query(err)){
            throw runtime_error("[ss] expected error [" + err + "]");
        }
        else{
            throw runtime_error("[ss] execution error [" + err + "]");
        }
    }

}

void ss_dut::reset(void){
    cout<<"ss reset"<<endl;
    dut -> reset();
}

void ss_dut::backup(void){
}

void ss_dut::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content){
    cout<<"ss get_content"<<endl;
    dut -> get_content(tables_name, content);
}

ss_dut::ss_dut(string db, string ip, unsigned int port,string single_dbms_name)
: db(db), ip(ip), port(port), single_dbms_name(single_dbms_name)
{
    if(single_dbms_name == "postgres"){
        dut = make_shared<dut_libpq>(db, ip, port);
    }
#ifdef HAVE_MYSQL
    else if(single_dbms_name == "mysql"){
        dut = make_shared<dut_mysql>(db, ip, port);
    }
#endif
    else{
        throw runtime_error("invalid single_dbms_name: " + single_dbms_name);
    }
}

ss_worker_connection::ss_worker_connection(string ip, string db, unsigned int port,string single_dbms_name, int id, string user, string password)
: test_ip(ip), test_db(db), test_port(port), id(id), user(user), password(password)
{
    connected_to_cluster = false;

    storage_unit = "ds_"+to_string(id);
    if(single_dbms_name == "postgres"){
        dut = make_shared<dut_libpq>(db, ip, port);
    }
#ifdef HAVE_MYSQL
    else if(single_dbms_name == "mysql"){
        dut = make_shared<dut_mysql>(db, ip, port);
    }
#endif
    else{
        throw runtime_error("invalid single_dbms_name: " + single_dbms_name);
    }
}

int extract_sharding_count(const std::string& shard_count_str) {
    //  "sharding-count":"4"  4
    static const regex re("\"sharding-count\":\"(\\d+)\"");

    smatch m;
    if (!regex_search(shard_count_str, m, re)) {
        throw std::runtime_error("sharding-count not found or not numeric: " + shard_count_str);
    }
    return std::stoi(m[1].str());
}

ss_schema::ss_schema(string db,string ip, unsigned int port,string single_dbms_name)
: single_dbms_name(single_dbms_name),db(db),ip(ip),port(port)
{
    dut = make_shared<ss_dut>(db, ip, port, single_dbms_name);
    target_ddbms = "ss";
    if(single_dbms_name == "postgres"){
        postgres_setup(db, ip, port);
    }
// #ifdef HAVE_MYSQL
//     else if(single_dbms_name == "mysql"){
//         mysql_setup(db, ip, port);
//     }
// #endif
}

void ss_schema::refresh(){
    if(single_dbms_name == "postgres"){
        postgres_setup(db, ip, port);
    }
// #ifdef HAVE_MYSQL
//     else if(single_dbms_name == "mysql"){
//         mysql_setup(db, ip, port);
//     }
// #endif
}

void ss_schema::postgres_setup(string db,string ip, unsigned int port){
    string version_sql = "select version();";
    vector<vector<string>> res;
    dut -> test(version_sql, &res);
    string version = res[0][0];
// cout<<"postgres version: "<<version<<endl;
//     string version_num_sql = "SHOW server_version_num;";
//     dut -> test(version_num_sql, &res);
//     string version_num_str = res[0][0];
// cout<<"postgres version_num_str: "<<version_num_str<<endl;
//     version_num = atoi(version_num_str.c_str());
    version_num = 170006;
// cout<<"postgres version_num: "<<version_num<<endl;
//     if (version_num == 0){
//         version_num = 120000;
//     }
    string procedure_is_aggregate = version_num < 110000 ? "proisagg" : "prokind = 'a'";
    string procedure_is_window = version_num < 110000 ? "proiswindow" : "prokind = 'w'";
    if(!has_types){
        throw runtime_error("should first call schema_pqxx() for has_types");
    }
    for (auto t : static_type_vec) {
        oid2type[t->oid_] = t;
        name2type[t->name] = t;
        types.push_back(t);
    }

    if (name2type.count("bool") > 0 &&  // no boolean type in pg_type
            name2type.count("int4") > 0 && // no integer type in pg_type
            name2type.count("numeric") > 0 &&
            name2type.count("text") > 0 &&
            name2type.count("timestamp") > 0) {
        
        booltype = name2type["bool"];
        inttype = name2type["int4"];
        realtype = name2type["numeric"];
        texttype = name2type["text"];
        datetype = name2type["timestamp"];
    }
    else {
        cerr << "at least one of booltype, inttype, realtype, texttype is not exist in" << debug_info << endl;
        throw runtime_error("at least one of booltype, inttype, realtype, texttype is not exist in" + debug_info);
    }

    internaltype = name2type["internal"];
    arraytype = name2type["anyarray"];
    true_literal = "true";
    false_literal = "false";
    null_literal = "null";

    compound_operators.push_back("union");
    compound_operators.push_back("union all");
    compound_operators.push_back("intersect");
    compound_operators.push_back("intersect all");
    compound_operators.push_back("except");
    compound_operators.push_back("except all");

    supported_join_op.push_back("cross");
    supported_join_op.push_back("inner");
    supported_join_op.push_back("left outer");
    supported_join_op.push_back("right outer");
    supported_join_op.push_back("full outer");
//     //todo: Planner Method Configuration
    target_dbms = "postgres";

    res.clear();
    string load_sharding_table_sql = "SHOW SHARDING TABLE RULES;";
    dut -> test(load_sharding_table_sql, &res);
    for (auto row : res) {
        string name = row[0];
        string column = row[8];
        string table_strategy = row[7]; // "STANDARD"
        string sharding_algorithm = row[9];// "hash_mod"
        string shard_count_str = row[10];// {"sharding-count":"4"}
        int shard_count = 5;//not important for non-modulo
        if(sharding_algorithm == "hash_mod" || sharding_algorithm == "mod")
            shard_count = extract_sharding_count(shard_count_str);
        cout<<"sharding table name: "<<name<<", column: "<<column<<", shard_count:"<<shard_count<<endl;
        auto st = make_shared<ss_table>(name, "public", true, true);
        if(!fill_table_column(st)){
            continue;
        }
        for(auto& col : st -> columns()){
            if(col.name == column){
                st->distribution_column = &col;
            }
        }
        cout << "dcolumn: "<< st->distribution_column->name << endl;
        st -> sharding_method = sharding_algorithm;
        st -> shard_count = shard_count;
        st -> ss_table_type = "distributed";
        tables.push_back(*st);
        distributed_tables[st->name] = st;

        dds d;
        d.params["victim"] = st->name;
        d.params["type"] = "2";
        d.params["colocate"] = "-1";
        d.params["shard_count"] = to_string(shard_count);
        d.params["dkey"] = st->distribution_column->name;
        this -> register_dds(st, d);
    }
    res.clear();
    string load_broadcast_table_sql = "SHOW BROADCAST TABLE RULES;";
    dut -> test(load_broadcast_table_sql, &res);
    for (auto row : res) {
        string name = row[0];
        auto bt = make_shared<ss_table>(name, "public", true, true);
        if(!fill_table_column(bt)){
            continue;
        }
        tables.push_back(*bt);
        broadcast_tables[bt->name] = bt;
        dds d;
        d.params["victim"] = bt->name;
        d.params["type"] = "1";
        d.params["colocate"] = "-1";
        d.params["shard_count"] = "-1";
        d.params["dkey"] = "<none>";
        this -> register_dds(bt, d);
    }
    res.clear();
    string load_single_table_sql = "SHOW SINGLE TABLES;";
    dut -> test(load_single_table_sql, &res);
    for (auto row : res) {
        string name = row[0];
        auto st = make_shared<ss_table>(name, "public", true, true);
        if(!fill_table_column(st)){
            continue;
        }
        st->single_table_storage = row[1];
        tables.push_back(*st);
        local_tables[st->name] = st;
        dds d;
        d.params["victim"] = st->name;
        d.params["type"] = "0";
        d.params["colocate"] = st->single_table_storage;
        d.params["shard_count"] = "1";
        d.params["dkey"] = "<none>";
        this -> register_dds(st, d);
    }
//     res.clear();
//     //find the metadata(column,type) of the sharding table
//     for (auto rule : rules){
//         string metadata_sql = "SHOW TABLE METADATA "+rule -> name+";";
//         dut -> test(metadata_sql, &res);
//         if(res.size() == 0){
//             rule -> exists = false;
//             continue;
//         }
//         table t(rule -> name, rule -> schema, rule -> is_insertable, rule -> is_base_table);
//         for (auto m_row : res) {
//             // string schema_name = m_row[0]; --not used
//             string type = m_row[2];
//             string metadata_name = m_row[3];
//             string value = m_row[4];
//             if(type == "COLUMN"){
//                 // metadata_name is the column name
//                 // extract the type from value
//                 regex datatype_pattern("\"dataType\":(\\d+)");
//                 smatch matches;
//                 column* c = NULL;
//                 if (regex_search(value, matches, datatype_pattern)) {
//                     int dataType = stoi(matches[1]);
//                     if(dataType == 4){
//                         c = new column(metadata_name, inttype);
//                     }else if(dataType == 2){
//                         c = new column(metadata_name, realtype);
//                     }else if(dataType == 12){
//                         c = new column(metadata_name, texttype);
//                     }else if(dataType == 93){
//                         c = new column(metadata_name, datetype);
//                     }else if (dataType == 16){
//                         c = new column(metadata_name, booltype);
//                     }
//                 }
//                 if(c){
//                     t.columns().push_back(*c);
//                 }
//                 //dynamic cast to sharding_table
//                 auto st = dynamic_pointer_cast<ss_sharding_table>(rule);
//                 if(st && metadata_name == st -> sharding_column_name){
//                     st -> sharding_column = c;
//                 }
//             }
//             if(type == "INDEX"){
//                 //todo
//             }
//         }
//     }

    for (auto& proc:static_routine_vec) {
        register_routine(proc);
    }

    for (auto &proc : routines) {
        auto& para_vec = static_routine_para_map[proc.specific_name];
        for (auto t:para_vec) {
            proc.argtypes.push_back(t);
        }
    }

    for (auto& proc:static_aggregate_vec) {
        register_aggregate(proc);
    }

    for (auto &proc : aggregates) {
        auto& para_vec = static_aggregate_para_map[proc.specific_name];
        for (auto t:para_vec) {
            proc.argtypes.push_back(t);
        }
    }

    generate_indexes();
    fill_table_versions();
}

bool ss_schema::fill_table_column(shared_ptr<ss_table> t){
    if(!t) throw runtime_error("null ptr in ss_schema::find_table");
    if(t->name == "") throw runtime_error("empty name in ss_schema::find_table");
    string show_table_metadata = "SHOW TABLE METADATA "+t->name+";";
    stmt_output res;
    dut -> test(show_table_metadata,&res);
    if(res.empty()){
        t->actually_exist = false;
        return false;
    }
    for (auto row : res){
        string metadata_name = row[3];
        string type = row[2];
        if(type == "COLUMN"){
            regex datatype_pattern("\"dataType\":(\\d+)");
            smatch matches;
            //row[4] is value
            if (regex_search(row[4], matches, datatype_pattern)) {
                // matches[1]  dataType 
                int dataType = stoi(matches[1]);
                column* c = NULL;
                if(dataType == 4){
                    c = new column(metadata_name, inttype);
                }else if(dataType == 2){
                    c = new column(metadata_name, realtype);
                }else if(dataType == 12){
                    c = new column(metadata_name, texttype);
                }else if(dataType == 93){
                    c = new column(metadata_name, datetype);
                }else if (dataType == 16){
                    c = new column(metadata_name, booltype);
                }

                if(c){
                    t->columns().push_back(*c);
                }
            }
        }else if(type == "INDEX"){

        }
    }
    return true;
}


// #ifdef HAVE_MYSQL
// void ss_schema::mysql_setup(string db,string ip, unsigned int port){
    
// }
// #endif



        // //find the metadata of the sharding table
        // string metadata_sql = "SHOW TABLE METADATA "+name+";";

        // vector<vector<string>> metadata_res;
        // dut -> test(metadata_sql, &metadata_res);
        // for (auto m_row : metadata_res) {
        //     string schema_name = m_row[0];
        //     string type = m_row[2];
        //     string metadata_name = m_row[3];
        //     string value = m_row[4];
        //     if(type == "COLUMN"){
        //         //metadata_name is the column name
        //         //extract the type from value
        //         regex datatype_pattern("\"dataType\":(\\d+)");
        //         smatch matches;
        //         if (regex_search(value, matches, datatype_pattern)) {
        //             // matches[1]  dataType 
        //             int dataType = stoi(matches[1]);
        //             if(dataType == 4){
                        
        //             }
        //         }
        //     }
        // }