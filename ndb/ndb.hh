#ifndef NDB_HH
#define NDB_HH
#include "../schema.hh"
#include "../relmodel.hh"
#include "../dut.hh"
#include "../context/context.hh"
#include <iomanip>
extern "C"  {
#include "../mysql.hh"
}
#include "nlohmann/json.hpp"
#define COLOCATE_NAME "colocated_key"

struct mysql_ndb_table : table{
    string engine;
    string partition_method;
    shared_ptr<column> dkey = nullptr;
    string partition_expr;
    int partition_count = 0;
    bool fully_replicated = false;
    mysql_ndb_table(string name, string schema, bool insertable, bool base_table)
    :table(name,schema,insertable,base_table){}
};

struct schema_mysql_ndb : schema, mysql_connection{
    vector<shared_ptr<mysql_ndb_table>> ndb_tables;
    schema_mysql_ndb(string db,string ip, unsigned int port);
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct ndb_context : Context{
    shared_ptr<schema_mysql_ndb> get_schema(void){
        return dynamic_pointer_cast<schema_mysql_ndb>(db_schema);
    }

    shared_ptr<dut_mysql> get_dut(void){
        return dynamic_pointer_cast<dut_mysql>(master_dut);
    }
};

// struct dut_mysql

#endif