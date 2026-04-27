#ifndef VITESS_HH
#define VITESS_HH
#include "../schema.hh"
#include "../relmodel.hh"
#include "../dut.hh"
#include "../context/context.hh"
#include "vtctldclient.hh"
#include <iomanip>
extern "C"  {
#include <mysql/mysql.h>
}
#include "nlohmann/json.hpp"
#define COLOCATE_NAME "colocated_key"

using namespace std;

std::map<std::string, pair<string,int>> read_vtctldclient_config(
    const std::string& path = "./vitess/vtctldclient_config"
);

// ---------------- Vitess VSchema typed model ----------------
// Goal: keep *table-level* DDS info inside vitess_table, while vschema provides
// keyspace-level context (sharded flag, vindex registry) and JSON (de)serialization.

enum class vitess_table_type {
    normal,
    reference,
    sequence
};

// A vindex instance registered under a keyspace (vschema.vindexes[name]).
struct vitess_vindex_def {
    std::string name;
    std::string type;               // e.g., "xxhash", "unicode_loose_xxhash", "lookup_unique"
    nlohmann::json params = nlohmann::json::object(); // extra fields other than "type"
    vitess_vindex_def(){};
    vitess_vindex_def(std::string name_,std::string type_):name(name_),type(type_){}

    nlohmann::json to_json() const;
    static vitess_vindex_def from_json(const std::string& name, const nlohmann::json& j);
};

// One entry in table.column_vindexes.
struct vitess_column_vindex {
    std::string column; // or empty if using "columns" form
    std::vector<std::string> columns;
    std::string name;   // reference to vschema.vindexes[*]
    nlohmann::json extra = nlohmann::json::object();

    vitess_column_vindex(){};
    vitess_column_vindex(string name_,string column_):name(name_),column(column_){};

    nlohmann::json to_json() const;
    static vitess_column_vindex from_json(const nlohmann::json& j);
};

// table.auto_increment
struct vitess_auto_increment {
    std::string column;    // auto-inc column name
    std::string sequence;  // fully qualified, e.g., "local.customer_seq"

    bool empty() const { return column.empty() && sequence.empty(); }
    nlohmann::json to_json() const;
    static vitess_auto_increment from_json(const nlohmann::json& j);
};

struct vitess_connection{
    string test_db;
    string test_ip;
    int test_port;
    MYSQL mysql;
    vitess_connection(string db,string ip, unsigned int port,bool distributed = true);
    ~vitess_connection(){
        mysql_close(&mysql);
    };
};

struct vitess_table : table{
    // ---- Table-level DDS info (what distranger needs at table granularity) ----
    // Keyspace where the table lives (Vitess: keyspace ~= MySQL database)
    std::string keyspace;
    // Keyspace-level sharding flag.
    bool keyspace_sharded = false;

    // Table kind (normal/reference/sequence).
    vitess_table_type type = vitess_table_type::normal;

    // Pinned keyspace id (base64) for "pinned" tables.
    std::string pinned;

    // For reference tables: source keyspace.table
    std::string source;

    // Vindex bindings.
    std::vector<vitess_column_vindex> column_vindexes;

    // auto_increment binding (needs sequences in Vitess for sharded tables)
    vitess_auto_increment auto_increment;

    // Preserve unknown table-level fields for round-trip.
    nlohmann::json extra = nlohmann::json::object();

    // Convenience for "primary" vindex (the first column_vindex).
    std::string primary_vindex_name() const {
        return column_vindexes.empty() ? std::string() : column_vindexes.front().name;
    }
    std::string primary_vindex_column() const {
        if (column_vindexes.empty()) return {};
        if (!column_vindexes.front().column.empty()) return column_vindexes.front().column;
        return column_vindexes.front().columns.empty() ? std::string() : column_vindexes.front().columns.front();
    }

    bool is_reference() const { return type == vitess_table_type::reference; }
    bool is_sequence()  const { return type == vitess_table_type::sequence; }
    bool is_pinned()    const { return !pinned.empty(); }

    // ---- VSchema JSON (de)serialization for this table object ----
    // Serialize into the *table object* part (vschema.tables[<name>]).
    nlohmann::json to_vschema_table_json() const;
    // Populate fields from vschema.tables[<name>].
    void from_vschema_table_json(const nlohmann::json& j,
                                 const std::string& keyspace_name,
                                 bool keyspace_is_sharded);

    vitess_table(string name, string schema, bool insertable, bool base_table)
    :table(name,schema,insertable,base_table), keyspace(std::move(schema)){}
};

struct vschema{
    // Keyspace name if available (GetVSchema often wraps it as {"keyspace":..., "vschema":...}).
    std::string keyspace;
    // keyspace-level sharding flag.
    bool sharded = false;

    // vindex registry for this keyspace.
    std::map<std::string, vitess_vindex_def> vindexes;

    // Preserve unknown keyspace-level fields for round-trip.
    nlohmann::json extra = nlohmann::json::object();

    // tables visible to distranger.
    vector<shared_ptr<vitess_table>> tables;

    //defalut null
    vschema(){};
    //build vschema from json string, vtctldclient::GetVSchemaString, should first fill in tables
    void fill_vschema(string json);
    //dump to json
    string to_json();

private:
    static nlohmann::json parse_json_loose(const std::string& s);
    static std::string table_type_to_string(vitess_table_type t);
    static vitess_table_type table_type_from_string(const std::string& s);
    
};

struct schema_vitess : schema, vitess_connection {
    map<string,vschema> vschema_map;
    schema_vitess(string db,string ip, unsigned int port, bool distributed = true);
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_vitess : dut_base, vitess_connection{
    bool has_sent_sql;
    string sent_sql;
    bool txn_abort;
    unsigned long thread_id;
    bool distributed;
    virtual void test(const string &stmt, 
        vector<vector<string>>* output = NULL, 
        int* affected_row_num = NULL,
        vector<string>* env_setting_stmts = NULL);
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);

    void block_test(const string &stmt, vector<vector<string>>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);
    dut_vitess(string db,string ip, unsigned int port,bool distributed = true);

    //reserved, do not implement
    virtual void backup(void){};
    virtual void reset_to_backup(void){};
};

struct vitess_context : Context{
    shared_ptr<schema_vitess> get_schema(void){
        return dynamic_pointer_cast<schema_vitess>(db_schema);
    }

    shared_ptr<dut_vitess> get_dut(void){
        return dynamic_pointer_cast<dut_vitess>(master_dut);
    }
};

#endif
