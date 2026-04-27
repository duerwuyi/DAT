/// @file
/// @brief Base class for device under test

#ifndef DUT_HH
#define DUT_HH
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include "dbms_info.hh"
#include "prod.hh"

using namespace std;

namespace dut {
  
struct failure : public std::exception {
  std::string errstr;
  std::string sqlstate;
  const char* what() const throw()
  {
    return errstr.c_str();
  }
  failure(const char *s, const char *sqlstate_ = "") throw()
       : errstr(), sqlstate() {
    errstr = s;
    sqlstate = sqlstate_;
  };
};

struct broken : failure {
  broken(const char *s, const char *sqlstate_ = "") throw()
    : failure(s, sqlstate_) { }
};

struct timeout : failure {
  timeout(const char *s, const char *sqlstate_ = "") throw()
    : failure(s, sqlstate_) { }
};

struct syntax : failure {
  syntax(const char *s, const char *sqlstate_ = "") throw()
    : failure(s, sqlstate_) { }
};

}

struct query_plan_operator{
    string oper;
    vector<string> properties;

    bool operator==(const query_plan_operator& other) const {
        return oper == other.oper;
    }
};

struct query_plan{
    std::vector<query_plan_operator> ops;

    bool operator==(const query_plan& other) const {
        if (ops.size() != other.ops.size()) return false;
        for (size_t i = 0; i < ops.size(); ++i) {
            if (!(ops[i] == other.ops[i])) return false; 
        }
        return true;
    }

    void clear() { ops.clear(); }
    size_t size() const { return ops.size(); }
    bool empty() const { return ops.empty(); }

    void add_operator(std::string oper, std::vector<std::string> params = {}) {
        ops.push_back(query_plan_operator{std::move(oper), std::move(params)});
    }

    auto begin() { return ops.begin(); }
    auto end()   { return ops.end(); }
    auto begin() const { return ops.begin(); }
    auto end()   const { return ops.end(); }
};


struct dut_base {
  std::string version;
  virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL) = 0;
  virtual void reset(void) = 0;

  virtual void backup(void) = 0;
  virtual void reset_to_backup(void) = 0;
  
  virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content) = 0;

  virtual vector<string> get_query_plan(const vector<vector<string>>& query_plan) {return vector<string>{""};};//return empty string if not supported
  // virtual string get_process_id() = 0;
};

#endif