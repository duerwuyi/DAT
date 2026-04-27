#ifndef CONTEXT_FACTORY_HH
#define CONTEXT_FACTORY_HH

#include "context.hh"
#include "../dbms_info.hh"
#include <memory>
#include <string>
#define DB_RECORD_FILE "db_setup.sql"
shared_ptr<Context> context_factory(dbms_info& d_info);

void save_query(string dir, string filename, string& query);
shared_ptr<dut_base> new_dut_setup(dbms_info& d_info);
shared_ptr<schema> get_new_schema(dbms_info& d_info);

#endif