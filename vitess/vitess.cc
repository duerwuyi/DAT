#include "vitess.hh"
#include <algorithm>
#include <sstream>
#include <unordered_map>
#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif
extern "C"  {
#include <unistd.h>
}

static regex e_unknown_database(".*Unknown database.*");
static regex e_db_dir_exists("[\\s\\S]*Schema directory[\\s\\S]*already exists. This must be resolved manually[\\s\\S]*");


static regex e_crash(".*Lost connection.*");
static regex e_dup_entry("Duplicate entry[\\s\\S]*for key[\\s\\S]*");
static regex e_large_results("Result of[\\s\\S]*was larger than max_allowed_packet[\\s\\S]*");
static regex e_timeout("Query execution was interrupted"); // timeout
static regex e_col_ambiguous("Column [\\s\\S]* in [\\s\\S]* is ambiguous");
static regex e_truncated("Truncated incorrect DOUBLE value:[\\s\\S]*");
static regex e_division_zero("Division by 0");
static regex e_unknown_col("Unknown column[\\s\\S]*"); // could be an unexpected error later
static regex e_incorrect_args("Incorrect arguments to[\\s\\S]*");
static regex e_out_of_range("[\\s\\S]*value is out of range[\\s\\S]*");
static regex e_win_context("You cannot use the window function[\\s\\S]*in this context[\\s\\S]*");
// same root cause of e_unknown_col, also could be an unexpected error later
static regex e_view_reference("[\\s\\S]*view lack rights to use them[\\s\\S]*");
static regex e_context_cancel("context canceled");
static regex e_string_convert("Cannot convert string[\\s\\S]*from binary to[\\s\\S]*");
static regex e_col_null("Column[\\s\\S]*cannot be null[\\s\\S]*");
static regex e_sridb_pk("Unsupported shard_row_id_bits for table with primary key as row id[\\s\\S]*");
static regex e_syntax("You have an error in your SQL syntax[\\s\\S]*");
static regex e_invalid_group("Invalid use of group function");
static regex e_invalid_group_2("In aggregated query without GROUP BY, expression[\\s\\S]*");
static regex e_oom("Out Of Memory Quota[\\s\\S]*");
static regex e_schema_changed("Information schema is changed during the execution of[\\s\\S]*");
static regex e_over_mem("[\\s\\S]*Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query[\\s\\S]*");
static regex e_no_default("Field [\\s\\S]* doesn't have a default value");
static regex e_no_group_by("Expression [\\s\\S]* of SELECT list is not in GROUP BY clause and contains nonaggregated column[\\s\\S]*");
static regex e_no_support_1("[\\s\\S]* not supported [\\s\\S]*");
static regex e_no_support_2("This version of MySQL doesn't yet support [\\s\\S]*");
static regex e_invalid_arguement("Invalid argument for [\\s\\S]*");
static regex e_incorrect_string("Incorrect string value: [\\s\\S]*");
static regex e_long_specified_key("Specified key was too long; max key length is [\\s\\S]* bytes");
static regex e_out_of_range_2("Out of range value for column [\\s\\S]*");
static regex e_table_not_exists("Table [\\s\\S]* doesn't exist");
static regex timeout("[\\s\\S]*time exceeded[\\s\\S]*");

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

static bool is_double(string myString, long double& result) {
    istringstream iss(myString);
    iss >> noskipws >> result; // noskipws considers leading whitespace invalid
    // Check the entire string was consumed and if either failbit or badbit is set
    return iss.eof() && !iss.fail(); 
}

static string process_an_item(string& item)
{
    string final_str;
    long double result;
    if (is_double(item, result) == false) {
        final_str = item;
    }
    else {
        if (result == 0) // result can be -0, represent it as 0
            final_str = "0";
        else {
            stringstream ss;
            int precision = 5;
            if (log10(result) > precision) // keep 5 valid number
                ss << setprecision(precision) << result;
            else // remove the number behind digit point
                ss << setiosflags(ios::fixed) << setprecision(0) << result;
            final_str = ss.str();
        }
    }
    return final_str;
}

std::map<std::string, pair<string,int>> read_vtctldclient_config(
    const std::string& path
){
    std::ifstream in(path);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open config file: " + path);
    }

    std::map<std::string, std::pair<std::string,int>> out;
    std::string line;
    int lineno = 0;

    while (std::getline(in, line)) {
        ++lineno;

        //  '#'  '//'
        if (auto p = line.find('#'); p != std::string::npos) line.resize(p);
        if (auto p = line.find("//"); p != std::string::npos) line.resize(p);

        std::istringstream iss(line);
        std::string name;
        std::string ip;
        int port = 0;

        if (!(iss >> name)) continue;               // /
        if (!(iss >> ip)) continue;               // /
        if (!(iss >> port)) {
            throw std::runtime_error("bad config at line " + std::to_string(lineno) +
                                     ": missing port");
        }
        if (port <= 0 || port > 65535) {
            throw std::runtime_error("bad config at line " + std::to_string(lineno) +
                                     ": invalid port " + std::to_string(port));
        }

        out[name] = {ip, port}; //  key 
    }

    return out;
}

// ---------------- VSchema JSON helpers ----------------

nlohmann::json vitess_vindex_def::to_json() const {
    nlohmann::json j = nlohmann::json::object();
    if (!type.empty()) j["type"] = type;
    for (auto it = params.begin(); it != params.end(); ++it) {
        j[it.key()] = it.value();
    }
    return j;
}

vitess_vindex_def vitess_vindex_def::from_json(const std::string& name, const nlohmann::json& j) {
    vitess_vindex_def def;
    def.name = name;
    if (j.is_object()) {
        if (j.contains("type") && j["type"].is_string()) def.type = j["type"].get<std::string>();
        for (auto it = j.begin(); it != j.end(); ++it) {
            if (it.key() == "type") continue;
            def.params[it.key()] = it.value();
        }
    }
    return def;
}

nlohmann::json vitess_column_vindex::to_json() const {
    nlohmann::json j = nlohmann::json::object();
    if (!column.empty()) j["column"] = column;
    if (!columns.empty()) j["columns"] = columns;
    if (!name.empty()) j["name"] = name;
    for (auto it = extra.begin(); it != extra.end(); ++it) {
        // avoid clobbering canonical fields
        if (it.key() == "column" || it.key() == "columns" || it.key() == "name") continue;
        j[it.key()] = it.value();
    }
    return j;
}

vitess_column_vindex vitess_column_vindex::from_json(const nlohmann::json& j) {
    vitess_column_vindex cv;
    if (!j.is_object()) return cv;
    if (j.contains("name") && j["name"].is_string()) cv.name = j["name"].get<std::string>();
    if (j.contains("column") && j["column"].is_string()) cv.column = j["column"].get<std::string>();
    if (j.contains("columns") && j["columns"].is_array()) {
        for (auto& e : j["columns"]) {
            if (e.is_string()) cv.columns.push_back(e.get<std::string>());
        }
    }
    // keep unknown fields (forward compatibility)
    for (auto it = j.begin(); it != j.end(); ++it) {
        if (it.key() == "column" || it.key() == "columns" || it.key() == "name") continue;
        cv.extra[it.key()] = it.value();
    }
    return cv;
}

nlohmann::json vitess_auto_increment::to_json() const {
    nlohmann::json j = nlohmann::json::object();
    if (!column.empty()) j["column"] = column;
    if (!sequence.empty()) j["sequence"] = sequence;
    return j;
}

vitess_auto_increment vitess_auto_increment::from_json(const nlohmann::json& j) {
    vitess_auto_increment ai;
    if (!j.is_object()) return ai;
    if (j.contains("column") && j["column"].is_string()) ai.column = j["column"].get<std::string>();
    if (j.contains("sequence") && j["sequence"].is_string()) ai.sequence = j["sequence"].get<std::string>();
    return ai;
}

nlohmann::json vitess_table::to_vschema_table_json() const {
    nlohmann::json j = nlohmann::json::object();

    // unknown fields first (so canonical fields can override)
    for (auto it = extra.begin(); it != extra.end(); ++it) {
        j[it.key()] = it.value();
    }

    // type
    if (type == vitess_table_type::reference) j["type"] = "reference";
    else if (type == vitess_table_type::sequence) j["type"] = "sequence";

    if (!source.empty()) j["source"] = source;
    if (!pinned.empty()) j["pinned"] = pinned;

    if (!column_vindexes.empty()) {
        nlohmann::json arr = nlohmann::json::array();
        for (auto& cv : column_vindexes) arr.push_back(cv.to_json());
        j["column_vindexes"] = std::move(arr);
    }

    if (!auto_increment.empty()) {
        j["auto_increment"] = auto_increment.to_json();
    }

    return j;
}

void vitess_table::from_vschema_table_json(const nlohmann::json& j,
                                          const std::string& keyspace_name,
                                          bool keyspace_is_sharded) {
    keyspace = keyspace_name;
    keyspace_sharded = keyspace_is_sharded;

    // reset table-level DDS fields and then fill
    type = vitess_table_type::normal;
    pinned.clear();
    source.clear();
    column_vindexes.clear();
    auto_increment = {};
    extra = nlohmann::json::object();

    if (!j.is_object()) return;

    if (j.contains("type") && j["type"].is_string()) {
        auto ts = j["type"].get<std::string>();
        if (ts == "reference") type = vitess_table_type::reference;
        else if (ts == "sequence") type = vitess_table_type::sequence;
    }
    if (j.contains("pinned") && j["pinned"].is_string()) pinned = j["pinned"].get<std::string>();
    if (j.contains("source") && j["source"].is_string()) source = j["source"].get<std::string>();

    if (j.contains("column_vindexes") && j["column_vindexes"].is_array()) {
        for (auto& e : j["column_vindexes"]) {
            column_vindexes.push_back(vitess_column_vindex::from_json(e));
        }
    }

    if (j.contains("auto_increment")) {
        auto_increment = vitess_auto_increment::from_json(j["auto_increment"]);
    }

    // preserve unknown keys
    for (auto it = j.begin(); it != j.end(); ++it) {
        const auto& k = it.key();
        if (k == "type" || k == "pinned" || k == "source" || k == "column_vindexes" || k == "auto_increment") continue;
        extra[k] = it.value();
    }
}

nlohmann::json vschema::parse_json_loose(const std::string& s) {
    // vtctldclient output is usually JSON, but may include newlines or other text.
    // We extract the first '{' and the last '}' to parse.
    auto b = s.find('{');
    auto e = s.rfind('}');
    if (b == std::string::npos || e == std::string::npos || e <= b) {
        throw std::runtime_error("GetVSchema output does not look like JSON");
    }
    auto sub = s.substr(b, e - b + 1);
    return nlohmann::json::parse(sub);
}

std::string vschema::table_type_to_string(vitess_table_type t) {
    switch (t) {
        case vitess_table_type::reference: return "reference";
        case vitess_table_type::sequence: return "sequence";
        case vitess_table_type::normal:
        default: return "";
    }
}

vitess_table_type vschema::table_type_from_string(const std::string& s) {
    if (s == "reference") return vitess_table_type::reference;
    if (s == "sequence") return vitess_table_type::sequence;
    return vitess_table_type::normal;
}

void vschema::fill_vschema(string json_str) {
    nlohmann::json root = parse_json_loose(json_str);

    // vtctldclient GetVSchema may wrap as {"keyspace":..., "vschema":{...}}
    nlohmann::json v = root;
    if (root.is_object()) {
        if (root.contains("keyspace") && root["keyspace"].is_string()) {
            keyspace = root["keyspace"].get<std::string>();
        }
        if (root.contains("vschema")) v = root["vschema"];
        else if (root.contains("VSchema")) v = root["VSchema"]; // defensive
    }

    if (!v.is_object()) {
        throw std::runtime_error("VSchema JSON is not an object");
    }

    sharded = v.value("sharded", false);

    // preserve unknown keyspace-level keys
    extra = nlohmann::json::object();
    for (auto it = v.begin(); it != v.end(); ++it) {
        const auto& k = it.key();
        if (k == "sharded" || k == "vindexes" || k == "tables") continue;
        extra[k] = it.value();
    }

    // vindex registry
    vindexes.clear();
    if (v.contains("vindexes") && v["vindexes"].is_object()) {
        for (auto it = v["vindexes"].begin(); it != v["vindexes"].end(); ++it) {
            vindexes[it.key()] = vitess_vindex_def::from_json(it.key(), it.value());
        }
    }

    // build lookup for existing tables (keyspace-qualified)
    std::unordered_map<std::string, std::shared_ptr<vitess_table>> idx;
    idx.reserve(tables.size());
    for (auto& tp : tables) {
        if (!tp) continue;
        idx[tp->keyspace + "." + tp->ident()] = tp;
    }

    // table entries
    if (v.contains("tables") && v["tables"].is_object()) {
        for (auto it = v["tables"].begin(); it != v["tables"].end(); ++it) {
            const std::string tname = it.key();
            const std::string ks = !keyspace.empty() ? keyspace : std::string();
            const std::string qn = (ks.empty() ? (std::string(".") + tname) : (ks + "." + tname));

            std::shared_ptr<vitess_table> tp;
            auto f = idx.find(qn);
            if (f != idx.end()) {
                tp = f->second;
            } else {
                // Not present in INFORMATION_SCHEMA snapshot; still keep it in vschema.
                auto created = std::make_shared<vitess_table>(tname, ks.empty() ? "" : ks, true, true);
                tables.push_back(created);
                tp = created;
                idx[qn] = tp;
            }

            // Note: table object can be null in JSON (rare) -> treat as empty object.
            nlohmann::json tj = it.value();
            if (tj.is_null()) tj = nlohmann::json::object();
            tp->from_vschema_table_json(tj, ks.empty() ? tp->keyspace : ks, sharded);
        }
    }
}

string vschema::to_json() {
    nlohmann::json out = nlohmann::json::object();

    // unknown fields first
    for (auto it = extra.begin(); it != extra.end(); ++it) {
        out[it.key()] = it.value();
    }
    out["sharded"] = sharded;

    if (!vindexes.empty()) {
        nlohmann::json vj = nlohmann::json::object();
        for (auto& [name, def] : vindexes) {
            vj[name] = def.to_json();
        }
        out["vindexes"] = std::move(vj);
    }

    nlohmann::json tj = nlohmann::json::object();
    for (auto& tp : tables) {
        if (!tp) continue;
        if (!keyspace.empty() && tp->keyspace != keyspace) continue;
        tj[tp->ident()] = tp->to_vschema_table_json();
    }
    out["tables"] = std::move(tj);

    return out.dump(2);
}


static int remove_dir(string dir) {
    string command = "rm -r " + dir;
    return system(command.c_str());
}

vitess_connection::vitess_connection(string db,string ip, unsigned int port,bool distributed)
{
    test_db = db;
    test_ip = ip;
    test_port = port;
    if (!mysql_init(&mysql))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    // password null: blank (empty) password field
    if (mysql_real_connect(&mysql, test_ip.c_str(), "user", NULL, test_db.c_str(), test_port, NULL, 0)) 
        return; // success
    
    string err = mysql_error(&mysql);
    if (!regex_match(err, e_unknown_database))
        throw std::runtime_error("BUG!!!" + string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    // error caused by unknown database, abort
    throw runtime_error("unknown database " + db +" in server=" + ip + ":" + to_string(port));
}

schema_vitess::schema_vitess(string db,string ip, unsigned int port,bool distributed)
:vitess_connection(db,ip,port,distributed){
    // Loading tables...;
    vector<shared_ptr<vitess_table>> vtables;
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + db + "' AND \
              TABLE_TYPE='BASE TABLE' ORDER BY 1;";
    
    if (mysql_real_query(&mysql, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    auto result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        // vitess_table tab(row[0], db, true, true);
        // tables.push_back(tab);
        vtables.push_back(make_shared<vitess_table>(row[0], db, true, true));
    }
    mysql_free_result(result);

    // if(db!="local"){
    //     string get_table_query2 = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
    //         WHERE TABLE_SCHEMA='local' AND \
    //             TABLE_TYPE='BASE TABLE' ORDER BY 1;";
        
    //     if (mysql_real_query(&mysql, get_table_query2.c_str(), get_table_query2.size()))
    //         throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
        
    //     result = mysql_store_result(&mysql);
    //     while (auto row = mysql_fetch_row(result)) {
    //         vitess_table tab(row[0], "local", true, true);
    //         tables.push_back(tab);
    //     }
    //     mysql_free_result(result);
    // }

    // // Loading views...;
    // string get_view_query = "select distinct table_name from information_schema.views \
    //     where (table_schema='" + db + "' OR table_schema='local') order by 1;";
    // if (mysql_real_query(&mysql, get_view_query.c_str(), get_view_query.size()))
    //     throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    // result = mysql_store_result(&mysql);
    // while (auto row = mysql_fetch_row(result)) {
    //     table tab(row[0], "main", false, false);
    //     tables.push_back(tab);
    // }
    // mysql_free_result(result);

    // Loading indexes...;
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE (TABLE_SCHEMA='" + db + "') AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&mysql, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);

    // if(db!="local"){
    //     vsch = vclient.GetVSchemaString("local");
    //     vschema_map["local"] = vschema();
    //     for(auto& t : tables){
    //         vschema_map["local"].tables.push_back(make_shared<vitess_table>(t));
    //     }
    //     vschema_map["local"].fill_vschema(vsch);
    // }

    // Loading columns and constraints...;
    for (auto& t : vtables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t->ident() + "' AND \
                    (TABLE_SCHEMA='" + db + "') ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info + "\nTable: " + t->ident());
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            string type =  row[1];
            if(type == "varchar")
                type = "varchar(100)";
            column c(row[0], sqltype::get(type));
            t->columns().push_back(c);
        }
        mysql_free_result(result);
    }

    //loading Vschema
    int vclient_port = read_vtctldclient_config()[distributed?"distributed":"single"].second;
    auto vclient = vtctldclient(ip,vclient_port);
    string vsch = vclient.GetVSchemaString(db);
    vschema_map[db] = vschema();
    for(auto& t : vtables){
        vschema_map[db].tables.push_back(shared_ptr<vitess_table>(t));
        this->tables.push_back((table)(*t));
    }
    vschema_map[db].fill_vschema(vsch);

    //loading dds;
    for(auto& [s ,v] : vschema_map){
        for(auto& t:v.tables){
            dds d;
            d.params["victim"] = t->name;
            if(s == "local") d.params["type"] = "local";
            else if(t->type == vitess_table_type::reference){
                d.params["type"] = "reference";
            }else if(t->pinned != ""){
                d.params["type"] = "pinned";
            }
            // else if(t->primary_vindex_column() == COLOCATE_NAME){
            //     d.params["type"] = "colocated";
            //     d.params["dkey"] = "colocated";
            //     d.params["shard_method"] = t->primary_vindex_name();
            // }
            else{
                d.params["type"] = "distributed";
                d.params["dkey"] = t->primary_vindex_column();
                d.params["shard_method"] = t->primary_vindex_name();
            }
            this -> register_dds(t, d);
        }
    }

    target_dbms = "mysql";
    target_ddbms = "vitess";

    booltype = sqltype::get("tinyint");
    inttype = sqltype::get("int");
    realtype = sqltype::get("double");
    texttype = sqltype::get("varchar(100)");
    datetype = sqltype::get("datetime");
    
    compound_operators.push_back("union distinct");
    compound_operators.push_back("union all");

    supported_join_op.push_back("inner");
    supported_join_op.push_back("left outer");
    supported_join_op.push_back("right outer");
    supported_join_op.push_back("cross");

    // bitwise
    BINOP(&, inttype, inttype, inttype);
    BINOP(>>, inttype, inttype, inttype);
    BINOP(<<, inttype, inttype, inttype);
    BINOP(^, inttype, inttype, inttype);
    BINOP(|, inttype, inttype, inttype);

    // comparison
    BINOP(>, inttype, inttype, booltype);
    BINOP(>, texttype, texttype, booltype);
    BINOP(>, realtype, realtype, booltype);
    BINOP(<, inttype, inttype, booltype);
    BINOP(<, texttype, texttype, booltype);
    BINOP(<, realtype, realtype, booltype);
    BINOP(>=, inttype, inttype, booltype);
    BINOP(>=, texttype, texttype, booltype);
    BINOP(>=, realtype, realtype, booltype);
    BINOP(<=, inttype, inttype, booltype);
    BINOP(<=, texttype, texttype, booltype);
    BINOP(<=, realtype, realtype, booltype);
    BINOP(<>, inttype, inttype, booltype);
    BINOP(<>, texttype, texttype, booltype);
    BINOP(<>, realtype, realtype, booltype);
    BINOP(!=, inttype, inttype, booltype);
    BINOP(!=, texttype, texttype, booltype);
    BINOP(!=, realtype, realtype, booltype);
    BINOP(<=>, inttype, inttype, booltype);
    BINOP(<=>, texttype, texttype, booltype);
    BINOP(<=>, realtype, realtype, booltype);
    BINOP(=, realtype, realtype, booltype);

    // arithmetic
    BINOP(%, inttype, inttype, inttype);
    BINOP(%, realtype, realtype, realtype);
    BINOP(*, inttype, inttype, inttype);
    BINOP(*, realtype, realtype, realtype);
    BINOP(+, inttype, inttype, inttype);
    BINOP(+, realtype, realtype, realtype);
    BINOP(-, inttype, inttype, inttype);
    BINOP(-, realtype, realtype, realtype);
    BINOP(/, inttype, inttype, inttype);
    BINOP(/, realtype, realtype, realtype);
    BINOP(DIV, inttype, inttype, inttype);
    BINOP(DIV, realtype, realtype, realtype);

    // logic
    BINOP(&&, booltype, booltype, booltype);
    BINOP(||, booltype, booltype, booltype);
    BINOP(XOR, booltype, booltype, booltype);

    // // comparison function
    // FUNC3(GREATEST, inttype, inttype, inttype, inttype);
    // FUNC3(GREATEST, realtype, realtype, realtype, realtype);
    // FUNC3(LEAST, inttype, inttype, inttype, inttype);
    // FUNC3(LEAST, realtype, realtype, realtype, realtype);
    // FUNC3(INTERVAL, inttype, inttype, inttype, inttype);
    // FUNC3(INTERVAL, inttype, realtype, realtype, realtype);

    // // function
    // FUNC1(ABS, inttype, inttype);
    // FUNC1(ABS, realtype, realtype);
    // FUNC1(ACOS, realtype, inttype);
    // FUNC1(ACOS, realtype, realtype);
    // FUNC1(ASIN, realtype, inttype);
    // FUNC1(ASIN, realtype, realtype);
    // FUNC1(ATAN, realtype, inttype);
    // FUNC1(ATAN, realtype, realtype);
    // FUNC2(ATAN, realtype, inttype, inttype);
    // FUNC2(ATAN, realtype, realtype, realtype);
    // FUNC2(ATAN2, realtype, inttype, inttype);
    // FUNC2(ATAN2, realtype, realtype, realtype);
    // FUNC1(CEILING, inttype, realtype);
    // FUNC1(COS, realtype, inttype);
    // FUNC1(COS, realtype, realtype);
    // FUNC1(COT, realtype, inttype);
    // FUNC1(COT, realtype, realtype);
    // FUNC1(CRC32, realtype, texttype);
    // FUNC1(DEGREES, realtype, inttype);
    // FUNC1(DEGREES, realtype, realtype);
    // FUNC1(EXP, realtype, inttype);
    // FUNC1(EXP, realtype, realtype);
    // FUNC1(FLOOR, inttype, realtype);
    // FUNC1(LN, realtype, inttype);
    // FUNC1(LN, realtype, realtype);
    // FUNC1(LOG, realtype, inttype);
    // FUNC1(LOG, realtype, realtype);
    // FUNC2(LOG, realtype, inttype, inttype);
    // FUNC2(LOG, realtype, realtype, realtype);
    // FUNC1(LOG2, realtype, inttype);
    // FUNC1(LOG2, realtype, realtype);
    // FUNC1(LOG10, realtype, inttype);
    // FUNC1(LOG10, realtype, realtype);
    // FUNC(PI, realtype);
    // FUNC2(POW, inttype, inttype, inttype);
    // FUNC2(POW, realtype, realtype, realtype);
    // FUNC1(RADIANS, realtype, inttype);
    // FUNC1(RADIANS, realtype, realtype);
    // FUNC1(ROUND, inttype, realtype);
    // FUNC1(SIGN, inttype, inttype);
    // FUNC1(SIGN, inttype, realtype);
    // FUNC1(SIN, realtype, inttype);
    // FUNC1(SIN, realtype, realtype);
    // FUNC1(SQRT, realtype, inttype);
    // FUNC1(SQRT, realtype, realtype);
    // FUNC1(TAN, realtype, inttype);
    // FUNC1(TAN, realtype, realtype);
    // FUNC2(TRUNCATE, realtype, realtype, inttype);

    // datetime operation
    // FUNC2(ADDDATE, datetype, datetype, inttype);
    // FUNC2(DATEDIFF, inttype, datetype, datetype);
    // // FUNC1(DATENAME, texttype, datetype); //FUNCTION testdb1.DATENAME does not exist
    // FUNC1(DAYOFMONTH, inttype, datetype);
    // FUNC1(DAYOFWEEK, inttype, datetype);
    // FUNC1(DAYOFYEAR, inttype, datetype);
    // FUNC1(HOUR, inttype, datetype);
    // FUNC1(MINUTE, inttype, datetype);
    // FUNC1(MONTH, inttype, datetype);
    // FUNC1(MONTHNAME, texttype, datetype);
    // FUNC1(QUARTER, inttype, datetype);
    // FUNC1(SECOND, inttype, datetype);
    // FUNC2(SUBDATE, datetype, datetype, inttype);
    // FUNC1(TIME_TO_SEC, inttype, datetype);
    // FUNC1(TO_DAYS, inttype, datetype);
    // FUNC1(TO_SECONDS, inttype, datetype);
    // FUNC1(UNIX_TIMESTAMP, inttype, datetype);
    // FUNC1(WEEK, inttype, datetype);
    // FUNC1(WEEKDAY, inttype, datetype);
    // FUNC1(WEEKOFYEAR, inttype, datetype);
    // FUNC1(YEAR, inttype, datetype);
    // FUNC1(YEARWEEK, inttype, datetype);

    // // string functions
    // FUNC1(ASCII, inttype, texttype);
    // FUNC1(BIN, texttype, inttype);
    // FUNC1(BIT_LENGTH, inttype, texttype);
    // FUNC1(CHAR_LENGTH, inttype, texttype);
    // FUNC2(CONCAT, texttype, texttype, texttype);
    // FUNC4(FIELD, inttype, texttype, texttype, texttype, texttype);
    // FUNC2(LEFT, texttype, texttype, inttype);
    // FUNC1(LENGTH, inttype, texttype);
    // FUNC1(HEX, texttype, texttype);
    // FUNC1(HEX, texttype, inttype);
    // FUNC2(INSTR, inttype, texttype, texttype);
    // FUNC2(LOCATE, inttype, texttype, texttype);
    // FUNC1(LOWER, texttype, texttype);
    // FUNC3(LPAD, texttype, texttype, inttype, texttype);
    // FUNC1(LTRIM, texttype, texttype);
    // FUNC4(MAKE_SET, texttype, inttype, texttype, texttype, texttype);
    // FUNC1(OCT, texttype, inttype);
    // FUNC1(ORD, inttype, texttype);
    // FUNC1(QUOTE, texttype, texttype);
    // FUNC2(REPEAT, texttype, texttype, inttype);
    // FUNC3(REPLACE, texttype, texttype, texttype, texttype);
    // FUNC1(REVERSE, texttype, texttype);
    // FUNC2(RIGHT, texttype, texttype, inttype);
    // FUNC3(RPAD, texttype, texttype, inttype, texttype);
    // FUNC1(RTRIM, texttype, texttype);
    // FUNC1(SOUNDEX, texttype, texttype);
    // FUNC1(SPACE, texttype, inttype);
    // FUNC3(SUBSTRING, texttype, texttype, inttype, inttype);
    // FUNC1(TO_BASE64, texttype, texttype);
    // FUNC1(TRIM, texttype, texttype);
    // // FUNC1(UNHEX, texttype, texttype); it return a binary string, which is a different type from string. 
    // // In case when, string and binary string will become binary string 
    // FUNC1(UPPER, texttype, texttype);
    // FUNC2(STRCMP, inttype, texttype, texttype);
    // // FUNC1(CHAR, texttype, inttype);

    // bit function
    // FUNC1(BIT_COUNT, inttype, inttype);

    // aggregate functions
    // AGG1(AVG, realtype, inttype);
    // AGG1(AVG, realtype, realtype);
    // AGG1(BIT_AND, inttype, inttype);
    // AGG1(BIT_OR, inttype, inttype);
    // AGG1(BIT_XOR, inttype, inttype);
    AGG(COUNT, inttype);
    AGG1(COUNT, inttype, realtype);
    AGG1(COUNT, inttype, texttype);
    AGG1(COUNT, inttype, inttype);
    AGG1(MAX, realtype, realtype);
    AGG1(MAX, inttype, inttype);
    AGG1(MIN, realtype, realtype);
    AGG1(MIN, inttype, inttype);
    // AGG1(STDDEV_POP, realtype, realtype);
    // AGG1(STDDEV_POP, realtype, inttype);
    // AGG1(STDDEV_SAMP, realtype, realtype);
    // AGG1(STDDEV_SAMP, realtype, inttype);
    // AGG1(SUM, realtype, realtype);
    // AGG1(SUM, inttype, inttype);
    // AGG1(VAR_POP, realtype, realtype);
    // AGG1(VAR_POP, realtype, inttype);
    // AGG1(VAR_SAMP, realtype, realtype);
    // AGG1(VAR_SAMP, realtype, inttype);

    // ranking window function
    // WIN(CUME_DIST, realtype);
    // WIN(DENSE_RANK, inttype);
    // // WIN1(NTILE, inttype, inttype);
    // WIN(RANK, inttype);
    // WIN(ROW_NUMBER, inttype);
    // WIN(PERCENT_RANK, realtype);

    // // value window function
    // WIN1(FIRST_VALUE, inttype, inttype);
    // WIN1(FIRST_VALUE, realtype, realtype);
    // WIN1(FIRST_VALUE, texttype, texttype);
    // WIN1(LAST_VALUE, inttype, inttype);
    // WIN1(LAST_VALUE, realtype, realtype);
    // WIN1(LAST_VALUE, texttype, texttype);

    // WIN1(LAG, inttype, inttype);
    // WIN1(LAG, realtype, realtype);
    // WIN1(LAG, texttype, texttype);
    // WIN2(LEAD, inttype, inttype, inttype);
    // WIN2(LEAD, realtype, realtype, inttype);
    // WIN2(LEAD, texttype, texttype, inttype);

    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("ARRAY");

    true_literal = "true";
    false_literal = "false";

    generate_indexes();
    fill_table_versions();
}

dut_vitess::dut_vitess(string db, string ip, unsigned int port,bool distributed)
  : vitess_connection(db,ip, port,distributed),distributed(distributed)
{
    sent_sql = "";
    has_sent_sql = false;
    txn_abort = false;
    thread_id = mysql_thread_id(&mysql);
    block_test("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
    string stmt = "SET MAX_EXECUTION_TIME = 6000;"; // 6 seconds
    if (mysql_real_query(&mysql, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&mysql);
        cerr << err << endl;
        // throw runtime_error(err + " in dut_tidb::dut_tidb"); 
    }
}

void dut_vitess::block_test(const string &stmt, 
    vector<vector<string>>* output, 
    int* affected_row_num)
{
    if(stmt.find("EXPLAIN") == string::npos &&
       stmt.find("SHOW") == string::npos
    ){
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        std::tm now_tm = *std::localtime(&now_time_t);
        std::cout<<" vitess test: "  << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S")
                << '.' << std::setfill('0') << std::setw(5) << now_ms.count() << std::endl;
        cout<<stmt.c_str()<<endl;
    }
    if (mysql_real_query(&mysql, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&mysql);
        if(err != "") cout<<err<<endl;
        auto result = mysql_store_result(&mysql);
        mysql_free_result(result);
        if (err.find("Commands out of sync") != string::npos) {// occasionally happens, retry the statement again
            cerr << err << " in test, repeat the statement again" << endl;
            block_test(stmt, output, affected_row_num);
            return;
        }
        if (regex_match(err, e_crash)) {
            throw std::runtime_error("BUG!!! " + err + " in mysql::block_test"); 
        }
        string prefix = "mysql block_test expected error:";
        if (regex_match(err, e_dup_entry) 
            || regex_match(err, e_large_results) 
            || regex_match(err, e_timeout) 
            || regex_match(err, e_col_ambiguous)
            || regex_match(err, e_truncated) 
            || regex_match(err, e_division_zero)
            || regex_match(err, e_unknown_col) 
            || regex_match(err, e_incorrect_args)
            || regex_match(err, e_out_of_range) 
            || regex_match(err, e_win_context)
            || regex_match(err, e_view_reference) 
            || regex_match(err, e_context_cancel)
            || regex_match(err, e_string_convert)
            // || regex_match(err, e_idx_oor)
            || regex_match(err, e_col_null)
            || regex_match(err, e_sridb_pk)
            || regex_match(err, e_syntax)
            // || regex_match(err, e_expr_pushdown)
            || regex_match(err, e_invalid_group)
            || regex_match(err, e_invalid_group_2)
            || regex_match(err, e_oom)
            // || regex_match(err, e_cannot_column)
            || regex_match(err, e_schema_changed)
            // || regex_match(err, e_invalid_addr)
            // || regex_match(err, e_makeslice)
            // || regex_match(err, e_undef_win)
            || regex_match(err, e_over_mem)
            || regex_match(err, e_no_default)
            || regex_match(err, e_no_group_by)
            || regex_match(err, e_no_support_1)
            || regex_match(err, e_no_support_2)
            || regex_match(err, e_invalid_arguement)
            || regex_match(err, e_incorrect_string)
            || regex_match(err, e_long_specified_key)
            || regex_match(err, e_out_of_range_2)
            || regex_match(err, e_table_not_exists)
            || regex_match(err, timeout)
           ) {
            throw runtime_error(prefix + err);
        }

        throw std::runtime_error("[" + err + "] in mysql::block_test"); 
    }

    if (affected_row_num)
        *affected_row_num = mysql_affected_rows(&mysql);

    auto result = mysql_store_result(&mysql);
    if (mysql_errno(&mysql) != 0) {
        string err = mysql_error(&mysql);
        if (regex_match(err, e_out_of_range)
            || regex_match(err, e_string_convert)
            || regex_match(err, e_table_not_exists)
            ) {
            throw runtime_error("mysql block_test/mysql_store_result expected error: " + err);
        }

        throw std::runtime_error("block_test: mysql_store_result fails [" + err + "]\nLocation: " + debug_info); 
    }

    if (output && result) {
        auto row_num = mysql_num_rows(result);
        if (row_num == 0) {
            mysql_free_result(result);
            return;
        }

        auto column_num = mysql_num_fields(result);
        while (auto row = mysql_fetch_row(result)) {
            vector<string> row_output;
            for (int i = 0; i < column_num; i++) {
                string str;
                if (row[i] == NULL)
                    str = "NULL";
                else
                    str = row[i];
                row_output.push_back(process_an_item(str));
                //row_output.push_back(str);
            }
            output->push_back(row_output);
        }
    }
    mysql_free_result(result);

    return;
}

void dut_vitess::test(const string &stmt, 
    vector<vector<string>>* output, 
    int* affected_row_num,
    vector<string>* env_setting_stmts)
{
    // if(stmt.find("DROP TABLE") != string::npos && stmt.find(";\n") != string::npos){
    //     auto[s1, s2] = split_at_first_newline1(stmt);
    //     block_test(s1, output, affected_row_num);
    //     block_test(s2, output, affected_row_num);
    // }else{
        block_test(stmt, output, affected_row_num);
    // }
    return;
}

void dut_vitess::reset() {
    // ---------- helpers ----------

    // auto ks_dir = [&](const std::string& ks) -> std::string {
    //     return std::string(VITESS_SAVED) + vtctldclient::SanitizePathComponent(ks) + "/";
    // };

    auto write_file = [&](const std::string& path, const std::string& content) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) throw std::runtime_error("reset: cannot write file: " + path);
        out << content;
        out.flush();
        if (!out) throw std::runtime_error("reset: write failed: " + path);
    };

    struct CmdRes { int rc; std::string out; };
    auto exec_capture = [&](const std::string& cmd) -> CmdRes {
        // capture stdout+stderr
        std::string full = cmd + " 2>&1";
        FILE* pipe = popen(full.c_str(), "r");
        if (!pipe) throw std::runtime_error("reset: popen failed: " + cmd);

        std::string out;
        char buf[4096];
        while (fgets(buf, sizeof(buf), pipe) != nullptr) out += buf;

        int rc = pclose(pipe);
        //  wait statusrc==0 
        return CmdRes{rc, out};
    };

    auto trim = [](std::string s) {
        auto is_ws = [](unsigned char c) { return c == ' ' || c == '\n' || c == '\r' || c == '\t'; };
        while (!s.empty() && is_ws((unsigned char)s.front())) s.erase(s.begin());
        while (!s.empty() && is_ws((unsigned char)s.back())) s.pop_back();
        return s;
    };

    // ---------- vtctld server config ----------
    std::string vt_ip = test_ip;
    int vt_port = read_vtctldclient_config()[distributed?"distributed":"single"].second;

    vtctldclient ctl(vt_ip, vt_port);
    const std::string server = vt_ip + ":" + std::to_string(vt_port);

    auto vt = [&](const std::string& subcmd) -> CmdRes {
        return exec_capture(ctl.bin + " --server " + server + " " + subcmd);
    };

    // ---------- workflow stop/delete ----------
    auto parse_workflows = [&](const std::string& out) -> std::vector<std::string> {
        std::vector<std::string> wfs;
        std::string t = trim(out);
        if (t.empty()) return wfs;

        // Try JSON first
        try {
            auto j = nlohmann::json::parse(t);
            if (j.is_array()) {
                for (auto& e : j) if (e.is_string()) wfs.push_back(e.get<std::string>());
                return wfs;
            }
            if (j.is_object()) {
                if (j.contains("workflows") && j["workflows"].is_array()) {
                    for (auto& e : j["workflows"]) {
                        if (e.is_string()) wfs.push_back(e.get<std::string>());
                        else if (e.is_object() && e.contains("name") && e["name"].is_string())
                            wfs.push_back(e["name"].get<std::string>());
                    }
                    return wfs;
                }
            }
        } catch (...) {
            // fall through to text
        }

        // Plain text: take first token of each non-header line
        std::istringstream iss(out);
        std::string line;
        while (std::getline(iss, line)) {
            line = trim(line);
            if (line.empty()) continue;
            // common "no workflows" messages
            if (line.find("No workflows") != std::string::npos ||
                line.find("no workflows") != std::string::npos) continue;
            // headers
            if (line.find("NAME") == 0 || line.find("Name") == 0 ||
                line.find("WORKFLOW") == 0 || line.find("Workflow") == 0) continue;

            std::istringstream lss(line);
            std::string tok;
            lss >> tok;
            if (tok.empty()) continue;
            wfs.push_back(tok);
        }
        return wfs;
    };

    auto list_workflows = [&](const std::string& ks) -> std::vector<std::string> {
        // Try several variants for compatibility across versions
        std::vector<std::string> cmds = {
            "workflow --keyspace " + ks + " list",
            // "Workflow --keyspace " + ks + " list",
        };
        CmdRes last{1, ""};
        for (auto& c : cmds) {
            auto r = vt(c);
            last = r;
            cerr<<c<<endl;
            if (r.rc == 0) return parse_workflows(r.out);
        }
        // If list itself cannot run, treat as hard error ( stop workflow)
        throw std::runtime_error("reset: workflow list failed for keyspace=" + ks + "\nOutput:\n" + last.out);
    };

    auto stop_and_delete_workflows = [&](const std::string& ks) {
        auto wfs = list_workflows(ks);
        for (auto& wf : wfs) {
            // stop (best-effort), then delete/cancel
            std::vector<std::string> stop_cmds = {
                "workflow --keyspace " + ks + " stop --workflow " + wf,
                "Workflow --keyspace " + ks + " stop --workflow " + wf
            };
            CmdRes stop_last{0, ""};
            bool stop_ok = false;
            for (auto& c : stop_cmds) {
                auto r = vt(c);
                stop_last = r;
                if (r.rc == 0) { stop_ok = true; break; }
            }
            // stop /

            std::vector<std::string> del_cmds = {
                "workflow --keyspace " + ks + " delete --workflow " + wf,
                "Workflow --keyspace " + ks + " delete --workflow " + wf,
                // // fallback: some versions use cancel/remove semantics
                // "workflow --keyspace " + ks + " cancel --workflow " + wf,
                // "Workflow --keyspace " + ks + " cancel --workflow " + wf
            };
            CmdRes del_last{0, ""};
            bool del_ok = false;
            for (auto& c : del_cmds) {
                auto r = vt(c);
                del_last = r;
                if (r.rc == 0) { del_ok = true; break; }
            }
            if (!del_ok) {
                throw std::runtime_error(
                    "reset: failed to delete/cancel workflow=" + wf + " keyspace=" + ks +
                    "\nLast output:\n" + del_last.out
                );
            }
        }
    };

    // ---------- choose keyspaces ----------
    std::vector<std::string> keyspaces;
    keyspaces.push_back(test_db);
    if (test_db != "local") keyspaces.push_back("local");

    // ---------- drop script ----------
    const std::string drop_table_sql_path = "./vitess/drop_t0_t100.sql";
    if (!std::filesystem::exists(drop_table_sql_path)) {
        throw std::runtime_error("reset: drop sql file not found: " + drop_table_sql_path);
    }

    // ---------- do reset per keyspace ----------
    for (const auto& ks : keyspaces) {
        // 1) stop & delete workflows
        stop_and_delete_workflows(ks);

        // 2) restore vschema to baseline (empty)
        std::string base_vschema;
        if (ks == "local") {
            base_vschema =
                "{\n"
                "  \"sharded\": false,\n"
                "  \"tables\": {}\n"
                "}\n";
        } else {
            base_vschema =
                "{\n"
                "  \"sharded\": true,\n"
                "  \"vindexes\": {},\n"
                "  \"tables\": {}\n"
                "}\n";
        }
        ctl.ApplyVSchemaFromString(ks, base_vschema);

        // 3) clear schema (drop tables)
        ctl.ApplySchemaFile(ks, drop_table_sql_path);

        // 4) keep local snapshots consistent with cleared DB
        // std::filesystem::create_directories(ctl.KeyspaceDir(ks));
        // write_file(ctl.SchemaPath() + VITESS_SCHEMA_FILE, "");
        // write_file(ctl.SchemaTmpPath(), "");
        // vschema snapshots are already updated by ApplyVSchemaFromString (tmp->committed)
    }
}

void dut_vitess::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

        if (mysql_real_query(&mysql, query.c_str(), query.size())) {
            string err = mysql_error(&mysql);
            cerr << "Cannot get content of " + table + "\nLocation: " + debug_info << endl;
            cerr << "Error: " + err + "\nLocation: " + debug_info << endl;
            continue;
        }

        auto result = mysql_store_result(&mysql);
        if (result) {
            auto column_num = mysql_num_fields(result);
            while (auto row = mysql_fetch_row(result)) {
                vector<string> row_output;
                for (int i = 0; i < column_num; i++) {
                    string str;
                    if (row[i] == NULL)
                        str = "NULL";
                    else
                        str = row[i];
                    row_output.push_back(process_an_item(str));
                }
                table_content.push_back(row_output);
            }
        }
        mysql_free_result(result);

        content[table] = table_content;
    }
}