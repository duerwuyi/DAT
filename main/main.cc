#include "../build/config.h"
#include <map>
#include <iostream>
#include <algorithm>
#include <cctype>
#include "../dbms_info.hh"
#include "../checker/common_fuzz_checker.hh"
#include "../context/context_factory.hh"
#include "../globals.h"
#include "../checker/ddc_checker.hh"
#include "../postgres.hh"


#include <thread>
#include <chrono>
#include <csignal>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

#define DEFAULT_DB_TEST_TIME 10

unsigned long long test_start_timestamp_ms = 0;
unsigned long long dbms_execution_ms = 0;
int executed_test_num = 0;
int rounds = 0;
int total_groups = 1;
bool flag1_citus = false;
bool flag1_ss = false;
volatile sig_atomic_t shutdown_requested = 0;

void request_shutdown(int) {
    shutdown_requested = 1;
}

bool feature_use_query_type = true;
bool feature_use_query_block = true;
bool feature_use_join_type = true;
bool feature_use_colocated_join = true;
bool feature_use_setop_type = true;
bool feature_use_subquery_type = true;
bool feature_use_outer_ref = true;
bool feature_use_modify_type = true;
bool feature_use_table_distribution = true;
bool feature_use_shard_routing = true;

bool prefix_matching = true;

int cpu_affinity = -1;
int max_level = 20; // max level of nested query
dbms_info d_info[2];


bool option_as_bool(map<string, string>& options, const string& key, bool default_value) {
    if (options.count(key) == 0) return default_value;
    string value = options[key];
    transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::tolower(c); });
    return !(value == "0" || value == "false" || value == "off" || value == "no");
}

void load_feature_options(map<string, string>& options) {
    feature_use_query_type = option_as_bool(options, "feature-query-type", true);
    feature_use_query_block = option_as_bool(options, "feature-query-block", true);
    feature_use_join_type = option_as_bool(options, "feature-join-type", true);
    feature_use_colocated_join = option_as_bool(options, "feature-colocated-join", true);
    feature_use_setop_type = option_as_bool(options, "feature-setop-type", true);
    feature_use_subquery_type = option_as_bool(options, "feature-subquery-type", true);
    feature_use_outer_ref = option_as_bool(options, "feature-outer-ref", true);
    feature_use_modify_type = option_as_bool(options, "feature-modify-type", true);
    feature_use_table_distribution = option_as_bool(options, "feature-table-distribution", true);
    feature_use_shard_routing = option_as_bool(options, "feature-shard-routing", true);

    cerr << "-------------Feature Extraction------------" << endl;
    cerr << "query type: " << feature_use_query_type << endl;
    cerr << "query block edges: " << feature_use_query_block << endl;
    cerr << "join type: " << feature_use_join_type << endl;
    cerr << "colocated join: " << feature_use_colocated_join << endl;
    cerr << "set operation type: " << feature_use_setop_type << endl;
    cerr << "subquery type: " << feature_use_subquery_type << endl;
    cerr << "outer reference edges: " << feature_use_outer_ref << endl;
    cerr << "modify type: " << feature_use_modify_type << endl;
    cerr << "table distribution: " << feature_use_table_distribution << endl;
    cerr << "shard routing: " << feature_use_shard_routing << endl;
    cerr << "------------------------------------------" << endl;
}

void load_options_from_file(const string& path, map<string, string>& options) {
    ifstream in(path);
    if (!in.is_open()) {
        cerr << "Cannot open config file: " << path << endl;
        exit(EXIT_FAILURE);
    }

    string line;
    while (getline(in, line)) {
        if (line.empty() || line[0] == '#') continue; // skip empty lines or comments
        size_t pos = line.find('=');
        if (pos == string::npos) {
            cerr << "Invalid config line: " << line << endl;
            continue;
        }
        string key = line.substr(0, pos);
        string value = line.substr(pos + 1);
        options[key] = value;
    }
}

int main(int argc, char *argv[]) {
    signal(SIGINT, request_shutdown);
    signal(SIGTERM, request_shutdown);
    
    // pipefd for process communication
    // int pipefd[2];
    // if (pipe(pipefd) == -1) {
    //     perror("pipe");
    //     exit(EXIT_FAILURE);
    // }

    // analyze the options
    map<string,string> options;

    if (argc <= 1) {
        cerr << "Usage: " << argv[0] << " <config_file>" << endl;
        exit(EXIT_FAILURE);
    }

    load_options_from_file(argv[1], options);
    load_feature_options(options);
    
    dbms_info info0(options,"0", false);
    dbms_info info1(options,"1", true);
    d_info[0] = info0;
    d_info[1] = info1;
    cerr << "-------------Test Info------------" << endl;
    cerr << "Test DBMS0: " << d_info[0].dbms_name << endl;
    cerr << "Test DBMS1: " << d_info[1].dbms_name << endl;
    cerr << "Test database0: " << d_info[0].test_db << endl;
    cerr << "Test database1: " << d_info[1].test_db << endl;
    cerr << "Test Distributed DBMS1: " << d_info[1].distributed_db_name << endl;
    cerr << "Test port0: " << d_info[0].test_port << endl;
    cerr << "Test port1: " << d_info[1].test_port << endl;
    cerr << "----------------------------------" << endl;
    int db_test_time = DEFAULT_DB_TEST_TIME;
    if (options.count("db-test-num") > 0)
        db_test_time = stoi(options["db-test-num"]);
    
    cpu_affinity = -1;
    if (options.count("cpu-affinity") > 0)
        cpu_affinity = stoi(options["cpu-affinity"]);

    if (cpu_affinity >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset); // clear the CPU set
        CPU_SET(cpu_affinity, &cpuset);

        if (sched_setaffinity(0, sizeof(cpuset), &cpuset) < 0) {
            cerr << "Failed to set CPU affinity" << endl;
            exit(EXIT_FAILURE);
        }
    }
    
    while (1) {
        cerr << "round " << rounds << " ... " << endl;
        rounds++;
        //clear all the files in "ddbms/saved"
        string cmd = "rm -f "+ d_info[1].distributed_db_name +"/saved/*";
        system(cmd.c_str());
        
        random_device rd;
        auto rand_seed = std::chrono::system_clock::now().time_since_epoch().count();
        // int64_t rand_seed = 1;
        if (options.count("seed") > 0)
            rand_seed = stoi(options["seed"]);
        cerr << "random seed: " << rand_seed << " ... " << endl;
        smith::rng.seed(rand_seed);
        auto ctx = context_factory(d_info[1]);
        auto compared = context_factory(d_info[0]);
        compared -> master_dut = new_dut_setup(compared -> info);
        if(options.count("ddc") > 0){
            cout << "ddc mode" << endl;
            {
                ddc_checker checker(ctx, compared, true, true, 1000);
                while(!shutdown_requested){
                    checker.run_ddc();
                }
                cerr << "shutdown requested, printing final ddc summary" << endl;
                checker.print();
            }
            clear_postgres_static_cache();
            sqltype::clear_cache();
            return 0;
        }
        else{
            cerr << "checker not supported" << endl;
            abort();
        }
        cerr << "done" << endl;
    }
    
    return 0;
}
