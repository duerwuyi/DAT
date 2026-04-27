#include "dbms_info.hh"

dbms_info::dbms_info(map<string,string>& options,string i,bool distri)
{    
    if (false) {}
    else if (options.count("mysql-db") && options.count("mysql-port"+ i)) {
        dbms_name = "mysql";
        test_port = stoi(options["mysql-port" + i]);
        test_ip = options["mysql-ip" + i];
        test_db = options["mysql-db"];
        can_trigger_error_in_txn = true;
        is_distributed = distri;
    }
    else if (options.count("postgres-db") && options.count("postgres-port" + i)) {
        dbms_name = "postgres";
        test_port = stoi(options["postgres-port" + i]);
        test_ip = options["postgres-ip" + i];
        test_db = options["postgres-db"];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }
    else if (options.count("postgres-db") && options.count("citus-port" + i)) {
        dbms_name = "postgres";
        distributed_db_name = "citus";
        test_port = stoi(options["citus-port" + i]);
        test_ip = options["citus-ip" + i];
        test_db = options["postgres-db"];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }
    else if (options.count("mysql-db") && options.count("tidb-port" + i)) {
        dbms_name = "mysql";
        distributed_db_name = "tidb";
        test_port = stoi(options["tidb-port" + i]);
        test_ip = options["tidb-ip" + i];
        test_db = options["mysql-db"];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }
    else if (options.count("postgres-db") && options.count("shardingsphere-port" + i)) {
        dbms_name = "postgres";
        distributed_db_name = "shardingsphere";
        test_port = stoi(options["shardingsphere-port" + i]);
        test_ip = options["shardingsphere-ip" + i];
        test_db = options["postgres-db"];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }
    else if (options.count("postgres-db") && options.count("tbase-port" + i)) {
        dbms_name = "postgres";
        distributed_db_name = "tbase";
        test_port = stoi(options["tbase-port" + i]);
        test_ip = options["tbase-ip" + i];
        test_db = options["postgres-db"];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }
    else if (options.count("clickhouse-db") && options.count("clickhouse-port" + i)) {
        dbms_name = "clickhouse";
        distributed_db_name = "clickhouse";
        test_port = stoi(options["clickhouse-port" + i]);
        test_ip = options["clickhouse-ip" + i];
        test_db = options["clickhouse-db"];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }
    else if (options.count("vitess-db" + i) && options.count("vitess-port" + i)) {
        dbms_name = "vitess";
        distributed_db_name = "vitess";
        test_port = stoi(options["vitess-port" + i]);
        test_ip = options["vitess-ip" + i];
        test_db = options["vitess-db" + i];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }else if (options.count("ndb-db" + i) && options.count("ndb-port" + i)) {
        dbms_name = "mysql_ndb";
        distributed_db_name = "mysql_ndb";
        test_port = stoi(options["ndb-port" + i]);
        test_ip = options["ndb-ip" + i];
        test_db = options["ndb-db" + i];
        can_trigger_error_in_txn = false;
        is_distributed = distri;
    }
    else {
        cerr << "Sorry,  you should specify a dbms and its database, or your dbms is not supported" << endl;
        throw runtime_error("Does not define target dbms and db");
    }

    if (options.count("output-or-affect-num")) 
        ouput_or_affect_num = stoi(options["output-or-affect-num"]);
    else 
        ouput_or_affect_num = 0;

    return;
}