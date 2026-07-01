#ifndef __SQL_PARSER_H_
#define __SQL_PARSER_H_

#include <string>
#include <vector>
#include "catalog.hpp"

// -------------------------------------------------------
// All supported query types
// -------------------------------------------------------
enum class QueryType {
    CREATE_TABLE,
    DROP_TABLE,
    ALTER_TABLE_ADD,
    INSERT,
    SELECT,
    JOIN,
    UPDATE,
    DELETE_FROM,
    EXPLAIN,
    SET_POLICY,
    UNKNOWN
};

// -------------------------------------------------------
// A single predicate: col op val  OR  col IS [NOT] NULL
//   or for JOIN:      t1.col op t2.col
// -------------------------------------------------------
struct Condition {
    std::string table1;     // left table  (empty for simple WHERE)
    std::string col1;       // left column
    std::string op;         // "=","!=","<>",">","<",">=","<=","LIKE","IS","IS NOT"
    std::string table2;     // right table (empty for simple WHERE)
    std::string col2;       // right column (empty for simple WHERE / literal)
    std::string value;      // literal value (empty for JOIN ON / IS NULL)

    bool isEmpty()  const { return col1.empty(); }
    bool isIsNull() const { return op == "IS" || op == "IS NOT"; }
};

// -------------------------------------------------------
// A WHERE clause: list of conditions joined by AND / OR
// -------------------------------------------------------
struct WhereClause {
    std::vector<Condition>   conditions;
    std::vector<std::string> connectors; // "AND" or "OR" between conditions
    // connectors.size() == conditions.size() - 1

    bool isEmpty() const { return conditions.empty(); }
};

// -------------------------------------------------------
// SET assignment for UPDATE
// -------------------------------------------------------
struct Assignment {
    std::string col;
    std::string value;
};

// -------------------------------------------------------
// Aggregate function in SELECT list
// -------------------------------------------------------
enum class AggFunc { NONE, COUNT, SUM, AVG, MIN, MAX };

struct SelectCol {
    AggFunc     agg  = AggFunc::NONE;
    std::string col;        // "*" for COUNT(*), else column name
    std::string alias;      // AS alias (optional)
    bool        star = false; // true when the whole SELECT list is *
};

// -------------------------------------------------------
// Everything the parser extracts from one SQL statement
// -------------------------------------------------------
struct ParsedQuery {
    QueryType type = QueryType::UNKNOWN;
    std::string error;

    // ---- CREATE TABLE ----
    std::string            table_name;
    std::vector<ColumnDef> columns;

    // ---- INSERT ----
    // table_name reused
    std::vector<std::string> col_names;   // optional column list
    std::vector<std::string> values;

    // ---- SELECT ----
    // table_name reused
    std::vector<SelectCol> select_cols;   // what to project
    bool                   distinct = false;
    WhereClause            where;
    std::vector<std::string> group_by;
    WhereClause            having;        // post-aggregate filter (simple)
    std::vector<std::string> order_by;    // column names
    std::vector<bool>        order_asc;   // true = ASC, false = DESC
    int limit  = -1;                      // -1 = no limit
    int offset =  0;

    // ---- JOIN ----
    std::string  table1, table2;
    Condition    on_cond;
    WhereClause  join_where;

    // ---- UPDATE ----
    // table_name reused
    std::vector<Assignment> assignments;
    WhereClause             update_where;

    // ---- DELETE ----
    // table_name reused
    WhereClause delete_where;

    // ---- EXPLAIN ----
    std::string explain_type;   // "SELECT" or "JOIN"

    // ---- SET POLICY ----
    int new_policy = 0;         // 1=LRU 2=MRU 3=CLOCK
};

// -------------------------------------------------------
// Parser
// -------------------------------------------------------
class SQLParser {
public:
    static ParsedQuery parse(const std::string& sql);

private:
    static std::vector<std::string> tokenize(const std::string& sql);
    static std::string toUpper(const std::string& s);
    static std::string toLower(const std::string& s);

    // Parses one condition starting at tokens[i]; advances i
    static Condition parseOneCondition(
        const std::vector<std::string>& tokens, size_t& i);

    // Parses a WHERE / HAVING clause (handles AND / OR chains)
    static WhereClause parseWhereClause(
        const std::vector<std::string>& tokens, size_t& i,
        const std::string& stopWord1 = "",
        const std::string& stopWord2 = "");
};

#endif // __SQL_PARSER_H_