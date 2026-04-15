#include "sqlParser.hpp"
#include <algorithm>
#include <cctype>
#include <stdexcept>

// -------------------------------------------------------
// Helpers
// -------------------------------------------------------
std::string SQLParser::toUpper(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), ::toupper);
    return r;
}

std::string SQLParser::toLower(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), ::tolower);
    return r;
}

// Tokenizer — handles:
//   quoted strings 'val' or "val" (strips quotes, preserves spaces inside)
//   two-char operators:  >= <= != <>
//   single-char tokens:  ( ) , ; = > <
//   everything else is whitespace-delimited
std::vector<std::string> SQLParser::tokenize(const std::string& sql) {
    std::vector<std::string> tokens;
    std::string cur;
    bool  in_quote  = false;
    char  quote_ch  = 0;

    for (size_t i = 0; i < sql.size(); ++i) {
        char c = sql[i];

        if (!in_quote && (c == '\'' || c == '"')) {
            in_quote = true; quote_ch = c; continue;
        }
        if (in_quote && c == quote_ch) {
            in_quote = false;
            tokens.push_back(cur); cur.clear(); continue;
        }
        if (in_quote) { cur += c; continue; }

        // Single-char punctuation
        if (c == '(' || c == ')' || c == ',' || c == ';') {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
            tokens.push_back(std::string(1, c));
            continue;
        }

        // Two-char operators: >= <= != <>
        if (i + 1 < sql.size()) {
            char nc = sql[i + 1];
            if ((c == '>' || c == '<' || c == '!') && nc == '=') {
                if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
                tokens.push_back(std::string(1, c) + "=");
                ++i; continue;
            }
            if (c == '<' && nc == '>') {
                if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
                tokens.push_back("<>");
                ++i; continue;
            }
        }

        // Single-char operators
        if (c == '=' || c == '>' || c == '<') {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
            tokens.push_back(std::string(1, c));
            continue;
        }

        if (isspace((unsigned char)c)) {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
            continue;
        }
        cur += c;
    }
    if (!cur.empty()) tokens.push_back(cur);
    return tokens;
}

// -------------------------------------------------------
// parseOneCondition
//   Handles:
//     col OP value               (simple)
//     col IS NULL
//     col IS NOT NULL
//     t.col OP t2.col            (JOIN ON)
//     col LIKE pattern
//     col != val  /  col <> val
// -------------------------------------------------------
Condition SQLParser::parseOneCondition(
    const std::vector<std::string>& tokens, size_t& i)
{
    Condition c;
    if (i >= tokens.size()) return c;

    // Left side: may be "table.col", "col", or "agg(col)"
    std::string lhs = tokens[i++];
    // Check for aggregate function call: next token is "("
    if (i < tokens.size() && tokens[i] == "(") {
        ++i; // skip (
        std::string inner;
        if (i < tokens.size() && tokens[i] != ")") {
            inner = tokens[i]; ++i;
        }
        if (i < tokens.size() && tokens[i] == ")") ++i; // skip )
        // Reconstruct as "agg(col)" — this is the header name used in HAVING
        lhs = lhs + "(" + inner + ")";
    }
    size_t dot = lhs.find('.');
    if (dot != std::string::npos) {
        c.table1 = toLower(lhs.substr(0, dot));
        c.col1   = toLower(lhs.substr(dot + 1));
    } else {
        c.col1 = toLower(lhs);
    }

    if (i >= tokens.size()) return c; // broken

    // Operator
    std::string op_raw = toUpper(tokens[i++]);

    // IS [NOT] NULL
    if (op_raw == "IS") {
        if (i < tokens.size() && toUpper(tokens[i]) == "NOT") {
            ++i;
            c.op    = "IS NOT";
            c.value = "NULL";
        } else {
            c.op    = "IS";
            c.value = "NULL";
        }
        if (i < tokens.size() && toUpper(tokens[i]) == "NULL") ++i;
        return c;
    }

    // Normalise != and <> to "!="
    if (op_raw == "<>") op_raw = "!=";
    c.op = op_raw;

    if (i >= tokens.size()) return c;

    // Right side: may be "table.col" or a literal
    std::string rhs = tokens[i++];
    size_t rdot = rhs.find('.');
    if (rdot != std::string::npos) {
        c.table2 = toLower(rhs.substr(0, rdot));
        c.col2   = toLower(rhs.substr(rdot + 1));
    } else {
        c.value = rhs; // literal
    }
    return c;
}

// -------------------------------------------------------
// parseWhereClause
//   Reads conditions joined by AND / OR until a stop word
//   (e.g. "ORDER", "GROUP", "LIMIT", "HAVING", or end of tokens)
// -------------------------------------------------------
WhereClause SQLParser::parseWhereClause(
    const std::vector<std::string>& tokens, size_t& i,
    const std::string& stopWord1,
    const std::string& stopWord2)
{
    WhereClause wc;
    while (i < tokens.size()) {
        std::string up = toUpper(tokens[i]);
        if (up == "ORDER" || up == "GROUP" || up == "LIMIT" ||
            up == "HAVING" || up == ")" || up == ";" ||
            (!stopWord1.empty() && up == stopWord1) ||
            (!stopWord2.empty() && up == stopWord2))
            break;

        Condition cond = parseOneCondition(tokens, i);
        wc.conditions.push_back(cond);

        if (i < tokens.size()) {
            std::string nxt = toUpper(tokens[i]);
            if (nxt == "AND" || nxt == "OR") {
                wc.connectors.push_back(nxt);
                ++i;
            } else {
                break;
            }
        }
    }
    return wc;
}

// -------------------------------------------------------
// parse
// -------------------------------------------------------
ParsedQuery SQLParser::parse(const std::string& sql) {
    ParsedQuery pq;
    auto tokens = tokenize(sql);
    if (tokens.empty()) { pq.error = "Empty query"; return pq; }

    size_t i = 0;
    std::string kw = toUpper(tokens[i]);

    // ====================================================
    // DROP TABLE t
    // ====================================================
    if (kw == "DROP") {
        ++i;
        if (i >= tokens.size() || toUpper(tokens[i]) != "TABLE") {
            pq.error = "Expected TABLE after DROP"; return pq;
        }
        ++i;
        if (i >= tokens.size()) { pq.error = "Expected table name"; return pq; }
        pq.type       = QueryType::DROP_TABLE;
        pq.table_name = toLower(tokens[i]);
        return pq;
    }

    // ====================================================
    // ALTER TABLE t ADD COLUMN col TYPE(size)
    // ====================================================
    if (kw == "ALTER") {
        ++i;
        if (i >= tokens.size() || toUpper(tokens[i]) != "TABLE") {
            pq.error = "Expected TABLE after ALTER"; return pq;
        }
        ++i;
        if (i >= tokens.size()) { pq.error = "Expected table name"; return pq; }
        pq.table_name = toLower(tokens[i]); ++i;
        if (i >= tokens.size() || toUpper(tokens[i]) != "ADD") {
            pq.error = "Only ADD is supported for ALTER TABLE"; return pq;
        }
        ++i;
        // Optional COLUMN keyword
        if (i < tokens.size() && toUpper(tokens[i]) == "COLUMN") ++i;

        ColumnDef col;
        if (i >= tokens.size()) { pq.error = "Expected column name"; return pq; }
        col.name = toLower(tokens[i]); ++i;
        if (i >= tokens.size()) { pq.error = "Expected type for column"; return pq; }
        std::string typeStr = toUpper(tokens[i]); ++i;
        if (typeStr == "INT" || typeStr == "INTEGER") {
            col.type = "INT"; col.size = 4;
        } else if (typeStr == "CHAR" || typeStr == "VARCHAR") {
            col.type = "CHAR"; col.size = 20;
            if (i < tokens.size() && tokens[i] == "(") {
                ++i;
                if (i < tokens.size() && tokens[i] != ")") {
                    col.size = std::stoi(tokens[i]); ++i;
                }
                if (i < tokens.size() && tokens[i] == ")") ++i;
            }
        } else {
            pq.error = "Unknown type: " + typeStr; return pq;
        }
        col.nullable = true;
        col.offset   = 0;
        pq.type = QueryType::ALTER_TABLE_ADD;
        pq.columns.push_back(col);
        return pq;
    }

    // ====================================================
    // CREATE TABLE t (col TYPE(size), ...)
    // ====================================================
    if (kw == "CREATE") {
        ++i;
        if (i >= tokens.size() || toUpper(tokens[i]) != "TABLE") {
            pq.error = "Expected TABLE after CREATE"; return pq;
        }
        ++i;
        if (i >= tokens.size()) { pq.error = "Expected table name"; return pq; }
        pq.type       = QueryType::CREATE_TABLE;
        pq.table_name = toLower(tokens[i]); ++i;

        if (i >= tokens.size() || tokens[i] != "(") {
            pq.error = "Expected '(' after table name"; return pq;
        }
        ++i;

        while (i < tokens.size() && tokens[i] != ")") {
            if (tokens[i] == ",") { ++i; continue; }
            ColumnDef col;
            col.name = toLower(tokens[i]); ++i;
            if (i >= tokens.size()) {
                pq.error = "Expected type for column " + col.name; return pq;
            }
            std::string typeStr = toUpper(tokens[i]); ++i;
            if (typeStr == "INT" || typeStr == "INTEGER") {
                col.type = "INT"; col.size = 4;
            } else if (typeStr == "CHAR" || typeStr == "VARCHAR") {
                col.type = "CHAR"; col.size = 20;
                if (i < tokens.size() && tokens[i] == "(") {
                    ++i;
                    if (i < tokens.size() && tokens[i] != ")")
                        { col.size = std::stoi(tokens[i]); ++i; }
                    if (i < tokens.size() && tokens[i] == ")") ++i;
                }
            } else {
                pq.error = "Unknown type: " + typeStr; return pq;
            }
            col.nullable = true;
            col.offset   = 0;
            pq.columns.push_back(col);
        }
        return pq;
    }

    // ====================================================
    // INSERT INTO t [(col,...)] VALUES (v1, ...)
    // ====================================================
    if (kw == "INSERT") {
        ++i;
        if (i >= tokens.size() || toUpper(tokens[i]) != "INTO") {
            pq.error = "Expected INTO after INSERT"; return pq;
        }
        ++i;
        if (i >= tokens.size()) { pq.error = "Expected table name"; return pq; }
        pq.type       = QueryType::INSERT;
        pq.table_name = toLower(tokens[i]); ++i;

        // Optional column list
        if (i < tokens.size() && tokens[i] == "(") {
            ++i;
            while (i < tokens.size() && tokens[i] != ")") {
                if (tokens[i] == ",") { ++i; continue; }
                pq.col_names.push_back(toLower(tokens[i])); ++i;
            }
            if (i < tokens.size()) ++i; // skip )
        }

        if (i >= tokens.size() || toUpper(tokens[i]) != "VALUES") {
            pq.error = "Expected VALUES"; return pq;
        }
        ++i;
        if (i >= tokens.size() || tokens[i] != "(") {
            pq.error = "Expected '(' after VALUES"; return pq;
        }
        ++i;
        while (i < tokens.size() && tokens[i] != ")") {
            if (tokens[i] == ",") { ++i; continue; }
            pq.values.push_back(tokens[i]); ++i;
        }
        return pq;
    }

    // ====================================================
    // UPDATE t SET col=val [, ...] [WHERE ...]
    // ====================================================
    if (kw == "UPDATE") {
        ++i;
        if (i >= tokens.size()) { pq.error = "Expected table name"; return pq; }
        pq.type       = QueryType::UPDATE;
        pq.table_name = toLower(tokens[i]); ++i;

        if (i >= tokens.size() || toUpper(tokens[i]) != "SET") {
            pq.error = "Expected SET"; return pq;
        }
        ++i;

        // Parse assignment list: col = val [, col = val ...]
        while (i < tokens.size() && toUpper(tokens[i]) != "WHERE") {
            if (tokens[i] == ",") { ++i; continue; }
            Assignment a;
            a.col = toLower(tokens[i]); ++i;
            if (i >= tokens.size() || tokens[i] != "=") {
                pq.error = "Expected '=' in SET clause"; return pq;
            }
            ++i;
            if (i >= tokens.size()) {
                pq.error = "Expected value in SET clause"; return pq;
            }
            a.value = tokens[i]; ++i;
            pq.assignments.push_back(a);
        }
        if (pq.assignments.empty()) {
            pq.error = "No assignments in SET clause"; return pq;
        }

        if (i < tokens.size() && toUpper(tokens[i]) == "WHERE") {
            ++i;
            pq.update_where = parseWhereClause(tokens, i);
        }
        return pq;
    }

    // ====================================================
    // DELETE FROM t [WHERE ...]
    // ====================================================
    if (kw == "DELETE") {
        ++i;
        if (i >= tokens.size() || toUpper(tokens[i]) != "FROM") {
            pq.error = "Expected FROM after DELETE"; return pq;
        }
        ++i;
        if (i >= tokens.size()) { pq.error = "Expected table name"; return pq; }
        pq.type       = QueryType::DELETE_FROM;
        pq.table_name = toLower(tokens[i]); ++i;

        if (i < tokens.size() && toUpper(tokens[i]) == "WHERE") {
            ++i;
            pq.delete_where = parseWhereClause(tokens, i);
        }
        return pq;
    }

    // ====================================================
    // SELECT [DISTINCT] col1 [AS a1], ... | * | agg(col) [AS a]
    //   FROM t [JOIN t2 ON ...]
    //   [WHERE ...]
    //   [GROUP BY col,...]
    //   [HAVING ...]
    //   [ORDER BY col [ASC|DESC] ,...]
    //   [LIMIT n [OFFSET m]]
    // ====================================================
    if (kw == "SELECT") {
        ++i;

        // DISTINCT
        if (i < tokens.size() && toUpper(tokens[i]) == "DISTINCT") {
            pq.distinct = true; ++i;
        }

        // Column list until FROM
        while (i < tokens.size() && toUpper(tokens[i]) != "FROM") {
            if (tokens[i] == ",") { ++i; continue; }
            if (tokens[i] == "*") {
                SelectCol sc; sc.star = true; sc.col = "*";
                pq.select_cols.push_back(sc);
                ++i; continue;
            }

            // Aggregate: COUNT( / SUM( / AVG( / MIN( / MAX(
            std::string up = toUpper(tokens[i]);
            AggFunc af = AggFunc::NONE;
            if      (up == "COUNT") af = AggFunc::COUNT;
            else if (up == "SUM")   af = AggFunc::SUM;
            else if (up == "AVG")   af = AggFunc::AVG;
            else if (up == "MIN")   af = AggFunc::MIN;
            else if (up == "MAX")   af = AggFunc::MAX;

            if (af != AggFunc::NONE) {
                ++i; // skip function name
                if (i < tokens.size() && tokens[i] == "(") {
                    ++i; // skip (
                    SelectCol sc;
                    sc.agg = af;
                    if (i < tokens.size() && tokens[i] != ")") {
                        sc.col = (tokens[i] == "*") ? "*" : toLower(tokens[i]);
                        ++i;
                    }
                    if (i < tokens.size() && tokens[i] == ")") ++i;
                    // Optional AS alias
                    if (i < tokens.size() && toUpper(tokens[i]) == "AS") {
                        ++i;
                        if (i < tokens.size()) { sc.alias = toLower(tokens[i]); ++i; }
                    }
                    pq.select_cols.push_back(sc);
                }
                continue;
            }

            // Plain column [AS alias]
            SelectCol sc;
            sc.agg = AggFunc::NONE;
            sc.col = toLower(tokens[i]); ++i;
            if (i < tokens.size() && toUpper(tokens[i]) == "AS") {
                ++i;
                if (i < tokens.size()) { sc.alias = toLower(tokens[i]); ++i; }
            }
            pq.select_cols.push_back(sc);
        }

        if (i >= tokens.size()) { pq.error = "Expected FROM"; return pq; }
        ++i; // skip FROM

        if (i >= tokens.size()) {
            pq.error = "Expected table name after FROM"; return pq;
        }
        std::string tname1 = toLower(tokens[i]); ++i;

        // JOIN?
        if (i < tokens.size() && toUpper(tokens[i]) == "JOIN") {
            ++i;
            pq.type   = QueryType::JOIN;
            pq.table1 = tname1;
            if (i >= tokens.size()) {
                pq.error = "Expected table after JOIN"; return pq;
            }
            pq.table2 = toLower(tokens[i]); ++i;

            if (i >= tokens.size() || toUpper(tokens[i]) != "ON") {
                pq.error = "Expected ON after JOIN table"; return pq;
            }
            ++i;
            pq.on_cond = parseOneCondition(tokens, i);

            if (i < tokens.size() && toUpper(tokens[i]) == "WHERE") {
                ++i;
                pq.join_where = parseWhereClause(tokens, i);
            }

            // ORDER BY for JOIN
            if (i < tokens.size() && toUpper(tokens[i]) == "ORDER") {
                ++i;
                if (i < tokens.size() && toUpper(tokens[i]) == "BY") ++i;
                while (i < tokens.size() &&
                       toUpper(tokens[i]) != "LIMIT" &&
                       toUpper(tokens[i]) != ";") {
                    if (tokens[i] == ",") { ++i; continue; }
                    pq.order_by.push_back(toLower(tokens[i])); ++i;
                    if (i < tokens.size()) {
                        std::string d = toUpper(tokens[i]);
                        if (d == "ASC")  { pq.order_asc.push_back(true);  ++i; }
                        else if (d == "DESC") { pq.order_asc.push_back(false); ++i; }
                        else pq.order_asc.push_back(true);
                    } else pq.order_asc.push_back(true);
                }
            }
            if (i < tokens.size() && toUpper(tokens[i]) == "LIMIT") {
                ++i;
                if (i < tokens.size()) { pq.limit = std::stoi(tokens[i]); ++i; }
                if (i < tokens.size() && toUpper(tokens[i]) == "OFFSET") {
                    ++i;
                    if (i < tokens.size()) { pq.offset = std::stoi(tokens[i]); ++i; }
                }
            }
            return pq;
        }

        // Simple SELECT
        pq.type       = QueryType::SELECT;
        pq.table_name = tname1;

        // WHERE
        if (i < tokens.size() && toUpper(tokens[i]) == "WHERE") {
            ++i;
            pq.where = parseWhereClause(tokens, i);
        }

        // GROUP BY
        if (i < tokens.size() && toUpper(tokens[i]) == "GROUP") {
            ++i;
            if (i < tokens.size() && toUpper(tokens[i]) == "BY") ++i;
            while (i < tokens.size() &&
                   toUpper(tokens[i]) != "HAVING" &&
                   toUpper(tokens[i]) != "ORDER"  &&
                   toUpper(tokens[i]) != "LIMIT"  &&
                   tokens[i] != ";") {
                if (tokens[i] == ",") { ++i; continue; }
                pq.group_by.push_back(toLower(tokens[i])); ++i;
            }
        }

        // HAVING
        if (i < tokens.size() && toUpper(tokens[i]) == "HAVING") {
            ++i;
            pq.having = parseWhereClause(tokens, i);
        }

        // ORDER BY
        if (i < tokens.size() && toUpper(tokens[i]) == "ORDER") {
            ++i;
            if (i < tokens.size() && toUpper(tokens[i]) == "BY") ++i;
            while (i < tokens.size() &&
                   toUpper(tokens[i]) != "LIMIT" &&
                   tokens[i] != ";") {
                if (tokens[i] == ",") { ++i; continue; }
                pq.order_by.push_back(toLower(tokens[i])); ++i;
                if (i < tokens.size()) {
                    std::string d = toUpper(tokens[i]);
                    if (d == "ASC")       { pq.order_asc.push_back(true);  ++i; }
                    else if (d == "DESC") { pq.order_asc.push_back(false); ++i; }
                    else pq.order_asc.push_back(true);
                } else pq.order_asc.push_back(true);
            }
        }

        // LIMIT [OFFSET]
        if (i < tokens.size() && toUpper(tokens[i]) == "LIMIT") {
            ++i;
            if (i < tokens.size()) {
                try { pq.limit = std::stoi(tokens[i]); ++i; }
                catch (...) { pq.error = "Invalid LIMIT value"; return pq; }
            }
            if (i < tokens.size() && toUpper(tokens[i]) == "OFFSET") {
                ++i;
                if (i < tokens.size()) {
                    try { pq.offset = std::stoi(tokens[i]); ++i; }
                    catch (...) { pq.error = "Invalid OFFSET value"; return pq; }
                }
            }
        }
        return pq;
    }

    // ---- EXPLAIN SELECT / EXPLAIN JOIN ----
    // Usage: EXPLAIN SELECT * FROM t
    //        EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.c = t2.c
    if (kw == "EXPLAIN") {
        pq.type = QueryType::EXPLAIN;
        // scan tokens for JOIN keyword
        bool is_join = false;
        for (size_t j = i; j < tokens.size(); ++j)
            if (toUpper(tokens[j]) == "JOIN") { is_join = true; break; }
        pq.explain_type = is_join ? "JOIN" : "SELECT";
        // extract table names by looking for FROM and JOIN tokens
        for (size_t j = i; j < tokens.size(); ++j) {
            std::string up = toUpper(tokens[j]);
            if (up == "FROM" && j+1 < tokens.size()) {
                if (!is_join) pq.table_name = tokens[j+1];
                else          pq.table1     = tokens[j+1];
            }
            if (up == "JOIN" && j+1 < tokens.size()) {
                pq.table2 = tokens[j+1];
            }
            if (up == "ON" && j+3 < tokens.size()) {
                // ON t1.col = t2.col  — store op and col refs
                pq.on_cond.col1 = tokens[j+1];
                pq.on_cond.op   = tokens[j+2];
                pq.on_cond.col2 = tokens[j+3];
            }
        }
        return pq;
    }

    // ---- SET POLICY ----
    // Usage: SET POLICY LRU | MRU | CLOCK
    if (kw == "SET") {
        ++i;  // skip past "SET"
        if (i < tokens.size() && toUpper(tokens[i]) == "POLICY") {
            ++i;  // skip past "POLICY"
            pq.type = QueryType::SET_POLICY;
            if (i < tokens.size()) {
                std::string pol = toUpper(tokens[i++]);
                if      (pol == "LRU")   pq.new_policy = 1;
                else if (pol == "MRU")   pq.new_policy = 2;
                else if (pol == "CLOCK") pq.new_policy = 3;
                else { pq.error = "Unknown policy: " + pol + " (use LRU, MRU, or CLOCK)"; }
            } else {
                pq.error = "Expected LRU, MRU, or CLOCK after SET POLICY";
            }
            return pq;
        }
        pq.error = "Only SET POLICY is supported";
        return pq;
    }

    pq.error = "Unrecognised SQL keyword: " + tokens[0];
    return pq;
}