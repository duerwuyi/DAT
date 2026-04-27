#include "ndb.hh"
#include <algorithm>
#include <sstream>
#include <unordered_map>
#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif
extern "C"  {
#include <unistd.h>
}
using namespace std;

static regex e_unknown_database(".*Unknown database.*");
static regex e_db_dir_exists("[\\s\\S]*Schema directory[\\s\\S]*already exists. This must be resolved manually[\\s\\S]*");

static regex e_crash(".*Lost connection.*");
static regex e_dup_entry("Duplicate entry[\\s\\S]*for key[\\s\\S]*");
static regex e_large_results("Result of[\\s\\S]*was larger than max_allowed_packet[\\s\\S]*");
static regex e_timeout("Query execution was interrupted"); // timeout
static regex e_col_ambiguous("Column [\\s\\S]* in [\\s\\S]* is ambiguous");
static regex e_truncated("Truncated incorrect DOUBLE value:[\\s\\S]*");
static regex e_division_zero("Division by 0");
static regex e_unknown_col("Unknown column[\\s\\S]*"); // could be an unexpected error later
static regex e_incorrect_args("Incorrect arguments to[\\s\\S]*");
static regex e_out_of_range("[\\s\\S]*value is out of range[\\s\\S]*");
static regex e_win_context("You cannot use the window function[\\s\\S]*in this context[\\s\\S]*");
// same root cause of e_unknown_col, also could be an unexpected error later
static regex e_view_reference("[\\s\\S]*view lack rights to use them[\\s\\S]*");
static regex e_context_cancel("context canceled");
static regex e_string_convert("Cannot convert string[\\s\\S]*from binary to[\\s\\S]*");
static regex e_col_null("Column[\\s\\S]*cannot be null[\\s\\S]*");
static regex e_sridb_pk("Unsupported shard_row_id_bits for table with primary key as row id[\\s\\S]*");
static regex e_syntax("You have an error in your SQL syntax[\\s\\S]*");
static regex e_invalid_group("Invalid use of group function");
static regex e_invalid_group_2("In aggregated query without GROUP BY, expression[\\s\\S]*");
static regex e_oom("Out Of Memory Quota[\\s\\S]*");
static regex e_schema_changed("Information schema is changed during the execution of[\\s\\S]*");
static regex e_over_mem("[\\s\\S]*Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query[\\s\\S]*");
static regex e_no_default("Field [\\s\\S]* doesn't have a default value");
static regex e_no_group_by("Expression [\\s\\S]* of SELECT list is not in GROUP BY clause and contains nonaggregated column[\\s\\S]*");
static regex e_no_support_1("[\\s\\S]* not supported [\\s\\S]*");
static regex e_no_support_2("This version of MySQL doesn't yet support [\\s\\S]*");
static regex e_invalid_arguement("Invalid argument for [\\s\\S]*");
static regex e_incorrect_string("Incorrect string value: [\\s\\S]*");
static regex e_long_specified_key("Specified key was too long; max key length is [\\s\\S]* bytes");
static regex e_out_of_range_2("Out of range value for column [\\s\\S]*");
static regex e_table_not_exists("Table [\\s\\S]* doesn't exist");
static regex timeout("[\\s\\S]*time exceeded[\\s\\S]*");

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

schema_mysql_ndb::schema_mysql_ndb(string db,string ip, unsigned int port)
  : mysql_connection(db,ip, port){
    // Loading tables...;
    string get_table_query =  R"SQL(SELECT
    t.TABLE_NAME,
    t.ENGINE,
    t.CREATE_OPTIONS,
    MAX(p.PARTITION_METHOD) AS partition_method,
    MAX(p.PARTITION_EXPRESSION) AS partition_expr,
    COUNT(DISTINCT p.PARTITION_NAME) AS partition_count
FROM INFORMATION_SCHEMA.TABLES t
LEFT JOIN INFORMATION_SCHEMA.PARTITIONS p
  ON p.TABLE_SCHEMA = t.TABLE_SCHEMA
 AND p.TABLE_NAME   = t.TABLE_NAME )SQL";
    get_table_query += "WHERE t.TABLE_SCHEMA = '" + db + "' GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, t.ENGINE, t.CREATE_OPTIONS;";
    // cout<<get_table_query<<endl;
    if (mysql_real_query(&mysql, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    auto result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        auto tab = make_shared<mysql_ndb_table>(row[0], db, true, true);
        tab -> engine = row[1];
        tab -> partition_method = row[3] == NULL ? "" : row[3];
        tab -> partition_count = stoi(row[5]);
        tab -> partition_expr = row[4] == NULL ? "" : row[4];
        ndb_tables.push_back(tab);
    }
    mysql_free_result(result);

    // // Loading views...;
    // string get_view_query = "select distinct table_name from information_schema.views \
    //     where table_schema='" + db + "' order by 1;";
    // if (mysql_real_query(&mysql, get_view_query.c_str(), get_view_query.size()))
    //     throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    // result = mysql_store_result(&mysql);
    // while (auto row = mysql_fetch_row(result)) {
    //     table tab(row[0], "main", false, false);
    //     tables.push_back(tab);
    // }
    // mysql_free_result(result);

    // Loading indexes...;
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE TABLE_SCHEMA='" + db + "' AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&mysql, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);

    // Loading columns and constraints...;
    for (auto& t : ndb_tables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t->ident() + "' AND \
                    TABLE_SCHEMA='" + db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info + "\nTable: " + t->ident());
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            string type =  row[1];
            if(type == "varchar")
                type = "varchar(100)";
            column c(row[0], sqltype::get(type));
            t->columns().push_back(c);
            if(t -> partition_expr== "`"+string(row[0])+"`"){
                t -> dkey == make_shared<column>(c);
            }
        }
        mysql_free_result(result);

        get_column_query = "SELECT  fully_replicated, read_backup, partition_balance \
        FROM ndbinfo.dictionary_tables \
        WHERE database_name = '"+db+"' AND table_name = '"+t->ident()+"';";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info + "\nTable: " + t->ident());
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            t->fully_replicated = row[0] == "1"?true:false;
        }
        mysql_free_result(result);
        dds d;
        d.params["victim"] = t->name;
        if(t->dkey)
            d.params["dkey"] = t->dkey->name;
        d.params["shard_method"] = t->partition_method;
        if(t->fully_replicated) d.params["type"] = "reference";
        else if(t->engine == "InnoDB") d.params["type"] = "local";
        else if(d.params["dkey"] == COLOCATE_NAME) d.params["type"] = "colocated";
        else d.params["type"] = "distributed";
        d.params["partition_count"] = t->partition_count;
        this -> register_dds(t, d);

        this->tables.push_back((table)(*t));
    }

    target_dbms = "mysql";
    target_ddbms = "mysql_ndb";

    booltype = sqltype::get("tinyint");
    inttype = sqltype::get("int");
    realtype = sqltype::get("double");
    texttype = sqltype::get("varchar(100)");
    datetype = sqltype::get("datetime");
    
    compound_operators.push_back("union distinct");
    compound_operators.push_back("union all");

    supported_join_op.push_back("inner");
    supported_join_op.push_back("left outer");
    supported_join_op.push_back("right outer");
    // supported_join_op.push_back("cross");

    // bitwise
    BINOP(&, inttype, inttype, inttype);
    BINOP(>>, inttype, inttype, inttype);
    BINOP(<<, inttype, inttype, inttype);
    BINOP(^, inttype, inttype, inttype);
    BINOP(|, inttype, inttype, inttype);

    // comparison
    BINOP(>, inttype, inttype, booltype);
    BINOP(>, texttype, texttype, booltype);
    BINOP(>, realtype, realtype, booltype);
    BINOP(<, inttype, inttype, booltype);
    BINOP(<, texttype, texttype, booltype);
    BINOP(<, realtype, realtype, booltype);
    BINOP(>=, inttype, inttype, booltype);
    BINOP(>=, texttype, texttype, booltype);
    BINOP(>=, realtype, realtype, booltype);
    BINOP(<=, inttype, inttype, booltype);
    BINOP(<=, texttype, texttype, booltype);
    BINOP(<=, realtype, realtype, booltype);
    BINOP(<>, inttype, inttype, booltype);
    BINOP(<>, texttype, texttype, booltype);
    BINOP(<>, realtype, realtype, booltype);
    BINOP(!=, inttype, inttype, booltype);
    BINOP(!=, texttype, texttype, booltype);
    BINOP(!=, realtype, realtype, booltype);
    BINOP(<=>, inttype, inttype, booltype);
    BINOP(<=>, texttype, texttype, booltype);
    BINOP(<=>, realtype, realtype, booltype);
    BINOP(=, realtype, realtype, booltype);

    // arithmetic
    BINOP(%, inttype, inttype, inttype);
    BINOP(%, realtype, realtype, realtype);
    BINOP(*, inttype, inttype, inttype);
    BINOP(*, realtype, realtype, realtype);
    BINOP(+, inttype, inttype, inttype);
    BINOP(+, realtype, realtype, realtype);
    BINOP(-, inttype, inttype, inttype);
    BINOP(-, realtype, realtype, realtype);
    BINOP(/, inttype, inttype, inttype);
    BINOP(/, realtype, realtype, realtype);
    BINOP(DIV, inttype, inttype, inttype);
    BINOP(DIV, realtype, realtype, realtype);

    // logic
    BINOP(&&, booltype, booltype, booltype);
    BINOP(||, booltype, booltype, booltype);
    BINOP(XOR, booltype, booltype, booltype);

    // comparison function
    FUNC3(GREATEST, inttype, inttype, inttype, inttype);
    FUNC3(GREATEST, realtype, realtype, realtype, realtype);
    FUNC3(LEAST, inttype, inttype, inttype, inttype);
    FUNC3(LEAST, realtype, realtype, realtype, realtype);
    FUNC3(INTERVAL, inttype, inttype, inttype, inttype);
    FUNC3(INTERVAL, inttype, realtype, realtype, realtype);

    // function
    FUNC1(ABS, inttype, inttype);
    FUNC1(ABS, realtype, realtype);
    FUNC1(ACOS, realtype, inttype);
    FUNC1(ACOS, realtype, realtype);
    FUNC1(ASIN, realtype, inttype);
    FUNC1(ASIN, realtype, realtype);
    FUNC1(ATAN, realtype, inttype);
    FUNC1(ATAN, realtype, realtype);
    FUNC2(ATAN, realtype, inttype, inttype);
    FUNC2(ATAN, realtype, realtype, realtype);
    FUNC2(ATAN2, realtype, inttype, inttype);
    FUNC2(ATAN2, realtype, realtype, realtype);
    FUNC1(CEILING, inttype, realtype);
    FUNC1(COS, realtype, inttype);
    FUNC1(COS, realtype, realtype);
    FUNC1(COT, realtype, inttype);
    FUNC1(COT, realtype, realtype);
    FUNC1(CRC32, realtype, texttype);
    FUNC1(DEGREES, realtype, inttype);
    FUNC1(DEGREES, realtype, realtype);
    FUNC1(EXP, realtype, inttype);
    FUNC1(EXP, realtype, realtype);
    FUNC1(FLOOR, inttype, realtype);
    FUNC1(LN, realtype, inttype);
    FUNC1(LN, realtype, realtype);
    FUNC1(LOG, realtype, inttype);
    FUNC1(LOG, realtype, realtype);
    FUNC2(LOG, realtype, inttype, inttype);
    FUNC2(LOG, realtype, realtype, realtype);
    FUNC1(LOG2, realtype, inttype);
    FUNC1(LOG2, realtype, realtype);
    FUNC1(LOG10, realtype, inttype);
    FUNC1(LOG10, realtype, realtype);
    FUNC(PI, realtype);
    FUNC2(POW, inttype, inttype, inttype);
    FUNC2(POW, realtype, realtype, realtype);
    FUNC1(RADIANS, realtype, inttype);
    FUNC1(RADIANS, realtype, realtype);
    FUNC1(ROUND, inttype, realtype);
    FUNC1(SIGN, inttype, inttype);
    FUNC1(SIGN, inttype, realtype);
    FUNC1(SIN, realtype, inttype);
    FUNC1(SIN, realtype, realtype);
    FUNC1(SQRT, realtype, inttype);
    FUNC1(SQRT, realtype, realtype);
    FUNC1(TAN, realtype, inttype);
    FUNC1(TAN, realtype, realtype);
    FUNC2(TRUNCATE, realtype, realtype, inttype);

    // datetime operation
    FUNC2(ADDDATE, datetype, datetype, inttype);
    FUNC2(DATEDIFF, inttype, datetype, datetype);
    // FUNC1(DATENAME, texttype, datetype); //FUNCTION testdb1.DATENAME does not exist
    FUNC1(DAYOFMONTH, inttype, datetype);
    FUNC1(DAYOFWEEK, inttype, datetype);
    FUNC1(DAYOFYEAR, inttype, datetype);
    FUNC1(HOUR, inttype, datetype);
    FUNC1(MINUTE, inttype, datetype);
    FUNC1(MONTH, inttype, datetype);
    FUNC1(MONTHNAME, texttype, datetype);
    FUNC1(QUARTER, inttype, datetype);
    FUNC1(SECOND, inttype, datetype);
    FUNC2(SUBDATE, datetype, datetype, inttype);
    FUNC1(TIME_TO_SEC, inttype, datetype);
    FUNC1(TO_DAYS, inttype, datetype);
    FUNC1(TO_SECONDS, inttype, datetype);
    FUNC1(UNIX_TIMESTAMP, inttype, datetype);
    FUNC1(WEEK, inttype, datetype);
    FUNC1(WEEKDAY, inttype, datetype);
    FUNC1(WEEKOFYEAR, inttype, datetype);
    FUNC1(YEAR, inttype, datetype);
    FUNC1(YEARWEEK, inttype, datetype);

    // string functions
    FUNC1(ASCII, inttype, texttype);
    FUNC1(BIN, texttype, inttype);
    FUNC1(BIT_LENGTH, inttype, texttype);
    FUNC1(CHAR_LENGTH, inttype, texttype);
    FUNC2(CONCAT, texttype, texttype, texttype);
    FUNC4(FIELD, inttype, texttype, texttype, texttype, texttype);
    FUNC2(LEFT, texttype, texttype, inttype);
    FUNC1(LENGTH, inttype, texttype);
    FUNC1(HEX, texttype, texttype);
    FUNC1(HEX, texttype, inttype);
    FUNC2(INSTR, inttype, texttype, texttype);
    FUNC2(LOCATE, inttype, texttype, texttype);
    FUNC1(LOWER, texttype, texttype);
    FUNC3(LPAD, texttype, texttype, inttype, texttype);
    FUNC1(LTRIM, texttype, texttype);
    FUNC4(MAKE_SET, texttype, inttype, texttype, texttype, texttype);
    FUNC1(OCT, texttype, inttype);
    FUNC1(ORD, inttype, texttype);
    FUNC1(QUOTE, texttype, texttype);
    FUNC2(REPEAT, texttype, texttype, inttype);
    FUNC3(REPLACE, texttype, texttype, texttype, texttype);
    FUNC1(REVERSE, texttype, texttype);
    FUNC2(RIGHT, texttype, texttype, inttype);
    FUNC3(RPAD, texttype, texttype, inttype, texttype);
    FUNC1(RTRIM, texttype, texttype);
    FUNC1(SOUNDEX, texttype, texttype);
    FUNC1(SPACE, texttype, inttype);
    FUNC3(SUBSTRING, texttype, texttype, inttype, inttype);
    FUNC1(TO_BASE64, texttype, texttype);
    FUNC1(TRIM, texttype, texttype);
    // FUNC1(UNHEX, texttype, texttype); it return a binary string, which is a different type from string. 
    // In case when, string and binary string will become binary string 
    FUNC1(UPPER, texttype, texttype);
    FUNC2(STRCMP, inttype, texttype, texttype);
    // FUNC1(CHAR, texttype, inttype);

    // bit function
    FUNC1(BIT_COUNT, inttype, inttype);

    // aggregate functions
    AGG1(AVG, realtype, inttype);
    AGG1(AVG, realtype, realtype);
    AGG1(BIT_AND, inttype, inttype);
    AGG1(BIT_OR, inttype, inttype);
    AGG1(BIT_XOR, inttype, inttype);
    AGG(COUNT, inttype);
    AGG1(COUNT, inttype, realtype);
    AGG1(COUNT, inttype, texttype);
    AGG1(COUNT, inttype, inttype);
    AGG1(MAX, realtype, realtype);
    AGG1(MAX, inttype, inttype);
    AGG1(MIN, realtype, realtype);
    AGG1(MIN, inttype, inttype);
    // AGG1(STDDEV_POP, realtype, realtype);
    // AGG1(STDDEV_POP, realtype, inttype);
    // AGG1(STDDEV_SAMP, realtype, realtype);
    // AGG1(STDDEV_SAMP, realtype, inttype);
    AGG1(SUM, realtype, realtype);
    AGG1(SUM, inttype, inttype);
    // AGG1(VAR_POP, realtype, realtype);
    // AGG1(VAR_POP, realtype, inttype);
    // AGG1(VAR_SAMP, realtype, realtype);
    // AGG1(VAR_SAMP, realtype, inttype);

    // ranking window function
    WIN(CUME_DIST, realtype);
    WIN(DENSE_RANK, inttype);
    // WIN1(NTILE, inttype, inttype);
    WIN(RANK, inttype);
    WIN(ROW_NUMBER, inttype);
    WIN(PERCENT_RANK, realtype);

    // value window function
    WIN1(FIRST_VALUE, inttype, inttype);
    WIN1(FIRST_VALUE, realtype, realtype);
    WIN1(FIRST_VALUE, texttype, texttype);
    WIN1(LAST_VALUE, inttype, inttype);
    WIN1(LAST_VALUE, realtype, realtype);
    WIN1(LAST_VALUE, texttype, texttype);
    // WIN1(LAG, inttype, inttype);
    // WIN1(LAG, realtype, realtype);
    // WIN1(LAG, texttype, texttype);
    // WIN2(LEAD, inttype, inttype, inttype);
    // WIN2(LEAD, realtype, realtype, inttype);
    // WIN2(LEAD, texttype, texttype, inttype);

    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("ARRAY");

    true_literal = "true";
    false_literal = "false";

    generate_indexes();
    fill_table_versions();
}
