#ifndef COMMON_FUZZ_CHECKER_HH
#define COMMON_FUZZ_CHECKER_HH

#include "../tester/ddc_tester.hh"
#include "../context/context.hh"
#include "../tester/diff_db_mutator.hh"
#include "../tester/cluster_fuzzer.hh"
// #include "../feedback/extract_feature.hh"
#include "../feedback/client.hh"
#include "../citus/citus_action.hh"
#include "../shardingsphere/ss_action.hh"
#include "../shardingsphere/ss_cluster_fuzzer.hh"
#include "../citus/citus_cluster_fuzzer.hh"
#include "../vitess/vitess_cluster.hh"
#include "../tidb/tidb_cluster.hh"
#include "../ndb/ndb_cluster.hh"
#include "../clickhouse/clickhouse_cluster.hh"
#include <chrono>
#include <string>

int save_backup_file(string path, dbms_info& d_info);

struct common_fuzz_checker{
    shared_ptr<diff_db_mutator> generator;
    shared_ptr<ddc_tester> tester;
    shared_ptr<cluster_fuzzer> fuzzer;

    shared_ptr<Context> compared;

    vector<shared_ptr<query>> valid_query_pool;
    vector<shared_ptr<query>> invalid_query_pool;

    int total_query_count = 1000;
    std::chrono::steady_clock::time_point checker_start_time = std::chrono::steady_clock::now();

    common_fuzz_checker(shared_ptr<Context> context, shared_ptr<Context> compared = nullptr,
         bool need_schema = true, bool should_change_schema = false, int total_query_count = 10000);

    void print_result();

    long long elapsed_runtime_seconds() const;
    string elapsed_runtime_hms() const;

    void run();

    void init_db();

    virtual void generate_query_pool();


    bool check(multiset<row_output>& r1, multiset<row_output>& r2);
    //differential testing
    bool run_and_check(shared_ptr<query> q, bool ensure_valid = false);

    string generate_vaild_query();

    void log(string s, string path = "selected_query.log");
};

#endif