/// @file
/// @brief schema and dut classes for PostgreSQL

#ifndef POSTGRES_HH
#define POSTGRES_HH

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "globals.h"
#include <pqxx/pqxx>

extern "C" {
#include <libpq-fe.h>
}

#define POSTGRES_BK_FILE(DB_NAME) ("/tmp/pg_" + DB_NAME + "_bk.sql")

#include <sys/time.h>
#include <fcntl.h>

#define POSTGRES_STMT_BLOCK_MS 200

#define OID long

struct pg_type : sqltype {
    OID oid_;
    char typdelim_;
    OID typrelid_;
    OID typelem_;
    OID typarray_;
    char typtype_;
    pg_type(string name,
        OID oid,
        char typdelim,
        OID typrelid,
        OID typelem,
        OID typarray,
        char typtype)
        : sqltype(name), oid_(oid), typdelim_(typdelim), typrelid_(typrelid),
          typelem_(typelem), typarray_(typarray), typtype_(typtype) { }
    virtual ~pg_type() {}
    virtual bool consistent(struct sqltype *rvalue);
    // bool consistent_(sqltype *rvalue);
};

extern bool has_types;
extern vector<pg_type *> static_type_vec;

extern bool has_operators;
extern vector<op> static_op_vec;

extern bool has_routines;
extern vector<routine> static_routine_vec;

extern bool has_routine_para;
extern map<string, vector<pg_type *>> static_routine_para_map;

extern bool has_aggregates;
extern vector<routine> static_aggregate_vec;

extern bool has_aggregate_para;
extern map<string, vector<pg_type *>> static_aggregate_para_map;

PGresult* pqexec_handle_error(PGconn *conn, string& query);
void log_query(const std::string& original_query);
void log(const std::string& original_query, const std::string& filepath);
void clear_postgres_static_cache();

struct pgsql_connection {
    PGconn *conn = 0;
    string PG_USER_NAME;
    string PG_PASSWORD;
    string test_ip;
    string test_db;
    unsigned int test_port;
    pgsql_connection(string db,string ip, unsigned int port);
    ~pgsql_connection();
};

struct schema_pqxx : schema, pgsql_connection {
    map<OID, pg_type*> oid2type;
    map<string, pg_type*> name2type;

    virtual string quote_name(const string &id) {
        return id;
    }
    bool is_consistent_with_basic_type(sqltype *rvalue);
    // schema_pqxx(string &conninfo, bool no_catalog);
    schema_pqxx(string db, string ip, unsigned int port, bool no_catalog);
    ~schema_pqxx();
};

// struct dut_pqxx : dut_base {
//     pqxx::connection c;
//     virtual void test(const std::string &stmt);
//     dut_pqxx(std::string conninfo);
// };

struct dut_libpq : dut_base, pgsql_connection {
    //bool is_distributed;
    //distributor* citus_distributor;
    virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);
    virtual void reset_without_force(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);    

    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);

    static int save_backup_file(string db_name, string path);
    virtual vector<string> get_query_plan(const vector<vector<string>>& query_plan) override;

    virtual void load_backup_schema(void);
    virtual void load_backup_data(void);
    virtual void load_single_backup_schema(void);
    virtual void load_single_backup_data(void);
    virtual void load_db_setup(void);
    dut_libpq(string db, string ip, unsigned int port);
};

#endif
