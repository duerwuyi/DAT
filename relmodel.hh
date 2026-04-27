/// @file
/// @brief supporting classes for the grammar

#ifndef RELMODEL_HH
#define RELMODEL_HH
#include <string>
#include <vector>
#include <map>
#include <utility>
#include <memory>
#include <cassert>
#include "globals.h"
#include "random.hh"
#include <unordered_map>
#include <set>
#include <nlohmann/json.hpp>

using std::string;
using std::vector;
using std::map;
using std::pair;
using std::make_pair;
using std::shared_ptr;
using std::multimap;
using std::exception;

struct sqltype {
    string name;
    static map<string, struct sqltype*> typemap;
    static struct sqltype *get(string s);
    static void clear_cache();
    sqltype(string n) : name(n) { }
    virtual ~sqltype() {}

    /** This function is used to model postgres-style pseudotypes.
        A generic type is consistent with a more concrete type.
        E.G., anyarray->consistent(intarray) is true
              while int4array->consistent(anyarray) is false

        There must not be cycles in the consistency graph, since the
        grammar will use fixpoint iteration to resolve type conformance
        situations in the direction of more concrete types  */
    virtual bool consistent(struct sqltype *rvalue);
};

struct routine {
    string specific_name;
    string schema;
    vector<sqltype *> argtypes;
    sqltype *restype;
    string name;
    routine(string schema, string specific_name, sqltype* data_type, string name)
        : specific_name(specific_name), schema(schema), restype(data_type), name(name) {
        assert(data_type);
    }
    
    virtual string ident() {
        if (schema.size())
            return schema + "." + name;
        else
            return name;
    }
};

struct column {
    string name;
    sqltype *type;
    routine *agg_used; // for columns that must be used with aggregate function (in having clause)
    // column(string name) : name(name) { }
    column(string name, sqltype *t, routine *agg = NULL) : name(name), agg_used(agg) {
        type = (agg == NULL) ? t : agg->restype;
        assert(t);
    }
};

struct relation {
    vector<column> cols;
    virtual vector<column> &columns() { return cols; }
};

struct named_relation : relation {
    string name;
    virtual string ident() { return name; }
    virtual ~named_relation() { }
    named_relation(string n) : name(n) { }
};

struct aliased_relation : named_relation {
  relation *rel;
  virtual ~aliased_relation() { }
  aliased_relation(string n, relation* r) : named_relation(n), rel(r) { }
  virtual vector<column>& columns() { return rel->columns(); }
};

struct table : named_relation {
    string schema;
    bool is_insertable;
    bool is_base_table;
    vector<string> constraints;
    table(string name, string schema, bool insertable, bool base_table)
        : named_relation(name),
        schema(schema),
        is_insertable(insertable),
        is_base_table(base_table) { }
    virtual string ident() { 
        // return schema + "." + name; 
        return name; }
    virtual ~table() { };

    virtual int get_type(){return 0;}

    int get_id_from_name() const {
        if (name[0] != 't') {
            return -1;
        }
        string version_id_str = name.substr(1);
        return std::stoi(version_id_str);
    }
};

int unique_relation_id();
void reset_relation_id_counter();

struct scope {
    struct scope *parent;
    /// available to index productions
    vector<string> indexes;
    /// available to table_ref productions
    vector<named_relation*> tables;
    /// available to column_ref productions
    vector<named_relation*> refs;
    struct schema *schema;
    //std::set<int> unused_table_groups;
    vector<table *> used_table_of_group;
    int nearest_from_clause_id;
    int nearest_from_clause_depth;
    // int relation_id = -1;
    /// Counters for prefixed stmt-unique identifiers
    shared_ptr<map<string,unsigned int> > stmt_seq;
    scope(struct scope *parent = 0) : parent(parent) {
        if (parent) {
            schema = parent->schema;
            tables = parent->tables;
            refs = parent->refs;
            stmt_seq = parent->stmt_seq;
            indexes = parent->indexes;
            //unused_table_groups = parent->unused_table_groups;
            used_table_of_group = parent->used_table_of_group;
            nearest_from_clause_id = parent->nearest_from_clause_id;
            nearest_from_clause_depth = parent->nearest_from_clause_depth;
            // relation_id = parent->relation_id;
        }
    }
    vector<pair<named_relation*, column> > refs_of_type(sqltype *t) {
        // cout << "t-type: " << t->name << endl;
        vector<pair<named_relation*, column> > result;
        for (auto r : refs)
            for (auto c : r->columns()){
                // cout << "c.type: " << c.type->name << endl;
                if (t->consistent(c.type))
	                result.push_back(make_pair(r,c));
            }
        return result;
    }
    /** Generate unique identifier with prefix. */
    string stmt_uid(const char* prefix) {
        string result(prefix);
        result += "_";
        result += std::to_string((*stmt_seq)[result]++);
        return result;
    }
    /** Reset unique identifier counters. */
    void new_stmt() {
        stmt_seq = std::make_shared<map<string, unsigned int> >();
        reset_relation_id_counter();
        nearest_from_clause_id = 0;
        nearest_from_clause_depth = 0;
    }
};

struct op {
    string name;
    sqltype *left;
    sqltype *right;
    sqltype *result;
    op(string n,sqltype *l,sqltype *r, sqltype *res)
        : name(n), left(l), right(r), result(res) { }
    op() { }
};

struct TableVersions {
    multimap<int, shared_ptr<table>> table_id_with_versions;

    void insert(shared_ptr<table> table_ptr);
    std::shared_ptr<table> random_remove(int id = -1);
    std::vector<std::shared_ptr<table>> clear_all_but_one_per_id();
    std::string allocate_next_table_name_for_id(int value);
};

struct dds{
    /*
    params:
    victim: table name
    type: table type
    dkey: dkey name(string)
    colocate: colocate table name(string) (citus only)
    shard_count: shard count(int)
    */
   std::unordered_map<std::string, std::string> params;
};

inline void to_json(nlohmann::json& j, const dds& d) {
    j = nlohmann::json{
        {"params", d.params}
    };
}

#endif
