-include Makefile.local

# bin_PROGRAMS = distranger gen_query exec_query gen_db
bin_PROGRAMS = distranger

AM_CPPFLAGS = -Dfuzz_test
DUT = postgres.cc
# if DUT_SQLITE
# DUT += sqlite.cc
# AM_CPPFLAGS += -DHAVE_SQLITE
# endif

# if DUT_TIDB
# DUT += tidb.cc
# AM_CPPFLAGS += -DHAVE_TIDB
# endif

# if DUT_MYSQL
# DUT += mysql.cc
# AM_CPPFLAGS += -DHAVE_MYSQL
# endif

DUT += mysql.cc

# DUT += clickhouse.cc
# AM_CPPFLAGS += -DHAVE_CLICKHOUSE

AM_CPPFLAGS += $(MYSQL_CFLAGS)

CONTEXT_SRC = context/context_factory.cc

# TESTER_SRC = tester/ddc_tester.cc tester/distri_db_generator.cc

TESTER_SRC = tester/ddc_tester.cc tester/diff_db_mutator.cc tester/cluster_fuzzer.cc 

CHECKER_SRC = checker/common_fuzz_checker.cc checker/ddc_checker.cc

FEEDBACK_SRC = feedback/action.cc feedback/client.cc feedback/extract_feature.cc

VALUE_EXPR_SRC = \
value_expr/atomic_subselect.cc \
value_expr/window_function.cc \
value_expr/value_expr.cc\
value_expr/coalesce.cc \
value_expr/funcall.cc \
value_expr/win_func_using_exist_win.cc \
value_expr/binop_expr.cc \
value_expr/const_expr.cc \
value_expr/case_expr.cc \
value_expr/win_funcall.cc \
value_expr/printed_expr.cc \
value_expr/column_reference.cc 

BOOL_EXPR_SRC = \
value_expr/bool_expr/comp_subquery.cc \
value_expr/bool_expr/not_expr.cc \
value_expr/bool_expr/like_op.cc \
value_expr/bool_expr/between_op.cc \
value_expr/bool_expr/null_predicate.cc \
value_expr/bool_expr/bool_expr.cc \
value_expr/bool_expr/exists_predicate.cc \
value_expr/bool_expr/const_bool.cc \
value_expr/bool_expr/in_query.cc \
value_expr/bool_expr/bool_binop/distinct_pred.cc \
value_expr/bool_expr/bool_binop/bool_term.cc \
value_expr/bool_expr/bool_binop/bool_binop.cc \
value_expr/bool_expr/bool_binop/comparison_op.cc

CITUS_SRC = \
citus/citus_action.cc \
citus/citus_cluster_fuzzer.cc \
citus/citus_sql_tester.cc \
citus/citus.cc

SS_SRC = \
shardingsphere/ss_cluster_fuzzer.cc \
shardingsphere/ss_sql_tester.cc \
shardingsphere/ss_action.cc \
shardingsphere/ss.cc

TIDB_SRC = \
tidb/tidb.cc \
tidb/tidb_cluster.cc

NDB_SRC = \
ndb/ndb.cc \
ndb/ndb_cluster.cc

CLICKHOUSE_SRC = \
clickhouse/clickhouse.cc \
clickhouse/clickhouse_cluster.cc


VITESS_SRC = \
vitess/vitess.cc \
vitess/vtctldclient.cc \
vitess/vitess_cluster.cc \
vitess/vitess_action.cc

AUTOMAKE_OPTIONS = subdir-objects

# add xx_SRC
common_src = relmodel.cc schema.cc $(DUT) $(CITUS_SRC) $(SS_SRC) $(TIDB_SRC) $(CLICKHOUSE_SRC) $(VITESS_SRC) $(NDB_SRC) \
    random.cc prod.cc $(VALUE_EXPR_SRC) $(BOOL_EXPR_SRC) grammar.cc log.cc dump.cc impedance.cc	\
    dbms_info.cc  $(CONTEXT_SRC) $(TESTER_SRC) $(CHECKER_SRC) $(FEEDBACK_SRC)

distranger_SOURCES = main/main.cc $(common_src)
#gen_query_SOURCES = main/gen_query.cc $(common_src)
#exec_query_SOURCES = main/exec_query.cc $(common_src)
#gen_db_SOURCES = main/gen_db.cc $(common_src)

distranger_LDADD = $(LIBPQXX_LIBS) $(MONETDB_MAPI_LIBS) $(BOOST_REGEX_LIB) $(POSTGRESQL_LIBS) $(BOOST_LDFLAGS) $(POSTGRESQL_LDFLAGS) $(MYSQL_LIBS)
#gen_query_LDADD = $(distranger_LDADD)
#exec_query_LDADD = $(distranger_LDADD)
#gen_db_LDADD = $(distranger_LDADD)

# AM_LDFLAGS = -fsanitize=address
AM_CPPFLAGS += $(BOOST_CPPFLAGS) $(LIBPQXX_CFLAGS) $(POSTGRESQL_CPPFLAGS) $(MONETDB_MAPI_CFLAGS) -Wall -Wno-sign-compare -Wextra -fPIC -g 

AM_CXXFLAGS = -g -O1 -fno-omit-frame-pointer
AM_LDFLAGS  =

distranger_CPPFLAGS = $(AM_CPPFLAGS)
#gen_query_CPPFLAGS = $(AM_CPPFLAGS)
#exec_query_CPPFLAGS = $(AM_CPPFLAGS)
#gen_db_CPPFLAGS = $(AM_CPPFLAGS)

#-O0 -fsanitize=address -fno-omit-frame-pointer
