#ifndef DBMS_INFO_HH
#define DBMS_INFO_HH

#include "config.h"
#include <string>
#include <map>
#include <iostream>

using namespace std;

struct dbms_info {
    string dbms_name;
    string test_db;
    int test_port;
    string test_ip;
    int ouput_or_affect_num;
    bool can_trigger_error_in_txn;
    bool is_distributed;
    string distributed_db_name;

    dbms_info(map<string,string>& options,string i,bool is_distributed);
    dbms_info() {
        dbms_name = "";
        test_db = "";
        test_ip = "";
        test_port = 0;
        ouput_or_affect_num = 0;
        can_trigger_error_in_txn = false;
    };
    void operator=(dbms_info& target) {
        dbms_name = target.dbms_name;
        test_db = target.test_db;
        test_port = target.test_port;
        test_ip = target.test_ip;
        ouput_or_affect_num = target.ouput_or_affect_num;
        can_trigger_error_in_txn = target.can_trigger_error_in_txn;
        is_distributed = target.is_distributed;
        distributed_db_name = target.distributed_db_name;
    }
};

#endif