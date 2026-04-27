// globals.h
#ifndef GLOBALS_H
#define GLOBALS_H

#include <csignal>

extern int executed_test_num;
extern int total_groups;
extern int rounds;
extern bool flag1_citus;
extern bool flag1_ss;
extern volatile sig_atomic_t shutdown_requested;

extern bool feature_use_query_type;
extern bool feature_use_query_block;
extern bool feature_use_join_type;
extern bool feature_use_colocated_join;
extern bool feature_use_setop_type;
extern bool feature_use_subquery_type;
extern bool feature_use_outer_ref;
extern bool feature_use_modify_type;
extern bool feature_use_table_distribution;
extern bool feature_use_shard_routing;

extern bool prefix_matching;

#endif // GLOBALS_H
