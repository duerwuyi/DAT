#ifndef DIFF_DB_MUTATOR_HH
#define DIFF_DB_MUTATOR_HH

#include "../dbms_info.hh"
#include "../schema.hh"
#include "../prod.hh"
#include "../grammar.hh"
#include "cluster_fuzzer.hh"
#include <functional>
#include "../context/context_factory.hh"
#include <sys/time.h>
#include <sys/wait.h>
#include "mysql.hh"

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#ifdef HAVE_COCKROACH
#include "cockroachdb.hh"
#endif
#include "postgres.hh"
// #include "clickhouse.hh"

struct diff_db_mutator{
    dbms_info distri_db_info;
    shared_ptr<Context> distri_context;
    dbms_info single_db_info;
    shared_ptr<Context> single_context;

    diff_db_mutator(shared_ptr<Context> distri_ctx, shared_ptr<Context> single_ctx);

    void elimitate_inconsistent_data(shared_ptr<cluster_fuzzer> fuzzer = nullptr);
    shared_ptr<schema> init_db();
    shared_ptr<schema> init_db_without_compare();

    // shared_ptr<schema> get_schema(){
    //     if(distri_context -> db_schema == nullptr) return nullptr;
    //     distri_context -> db_schema -> refresh();
    //     return distri_context -> db_schema;
    // };
};

#endif