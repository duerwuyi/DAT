#ifndef CONTEXT_HH
#define CONTEXT_HH

#include "../dut.hh"
#include "../schema.hh"
#include "../dbms_info.hh"
#include <memory>
#include <string>

struct Context {
    int thread_id;
    shared_ptr<dut_base> master_dut;
    shared_ptr<schema> db_schema;
    dbms_info info;
    string logfile;
    virtual ~Context() {}
};

#endif