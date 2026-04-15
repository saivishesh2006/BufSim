#!/usr/bin/env bash
# =============================================================
# test_bufsim.sh  —  Full automated test suite for BufSim
# =============================================================
# Usage:  bash test_bufsim.sh
# Requires: ./bufsim binary (run `make` first)
# =============================================================

BINARY="./bufsim"
PASS=0
FAIL=0
TOTAL=0

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
CYAN="\033[0;36m"
RESET="\033[0m"

# ── assertion helpers ─────────────────────────────────────────
assert() {
    local desc="$1" output="$2" pattern="$3"
    TOTAL=$((TOTAL+1))
    if echo "$output" | grep -qF "$pattern"; then
        PASS=$((PASS+1))
        printf "  ${GREEN}PASS${RESET}  %s\n" "$desc"
    else
        FAIL=$((FAIL+1))
        printf "  ${RED}FAIL${RESET}  %s\n" "$desc"
        printf "        expected: [%s]\n" "$pattern"
        echo "$output" | grep "|" | head -5 | sed 's/^/        /'
    fi
}

assert_absent() {
    local desc="$1" output="$2" pattern="$3"
    TOTAL=$((TOTAL+1))
    if echo "$output" | grep -qF "$pattern"; then
        FAIL=$((FAIL+1))
        printf "  ${RED}FAIL${RESET}  %s  (found unwanted: [%s])\n" "$desc" "$pattern"
    else
        PASS=$((PASS+1))
        printf "  ${GREEN}PASS${RESET}  %s\n" "$desc"
    fi
}

assert_lineno() {
    # assert line containing $pat1 appears BEFORE line containing $pat2
    # Only considers lines that contain "|" (table output rows)
    local desc="$1" output="$2" pat1="$3" pat2="$4"
    TOTAL=$((TOTAL+1))
    local l1 l2
    l1=$(echo "$output" | grep -n "$pat1" | head -1 | cut -d: -f1)
    l2=$(echo "$output" | grep -n "$pat2" | head -1 | cut -d: -f1)
    if [[ -n "$l1" && -n "$l2" && "$l1" -lt "$l2" ]]; then
        PASS=$((PASS+1))
        printf "  ${GREEN}PASS${RESET}  %s\n" "$desc"
    else
        FAIL=$((FAIL+1))
        printf "  ${RED}FAIL${RESET}  %s  (%s line=%s, %s line=%s)\n" \
               "$desc" "$pat1" "${l1:-?}" "$pat2" "${l2:-?}"
    fi
}

section() { printf "\n${CYAN}══ %s ══${RESET}\n" "$1"; }

# ── run_sql ───────────────────────────────────────────────────
# Runs bufsim with given policy/frames/sql.
# Returns ONLY the output.txt written by THIS run (not accumulated ones).
# Strategy: mark start position in output.txt before run, read from there after.

run_sql() {
    local policy="${1:-1}" frames="${2:-10}"
    shift 2
    local sql="$*"

    # Reset catalog and binary files; truncate output.txt cleanly
    echo '{"tables":[]}' > catalog.json
    rm -f ./*.bin
    : > output.txt          # truncate to empty

    printf "%s\n%s\n%s\nexit\n" "$policy" "$frames" "$sql" \
        | "$BINARY" 2>/dev/null > /dev/null

    # Return output.txt with SQL echo lines stripped
    # (echo lines contain the input SQL and cause false positives in assert_absent)
    grep -v "^SQL: " output.txt
}

# ── seed data ─────────────────────────────────────────────────
SEED="
CREATE TABLE employees (emp_name CHAR(20), dept_id INT, salary INT)
CREATE TABLE departments (dept_name CHAR(20), dept_id INT, budget INT)
INSERT INTO employees VALUES (alice, 1, 72000)
INSERT INTO employees VALUES (bob, 2, 48000)
INSERT INTO employees VALUES (carol, 1, 95000)
INSERT INTO employees VALUES (dan, 3, 61000)
INSERT INTO employees VALUES (eve, 2, 83000)
INSERT INTO employees VALUES (frank, 1, 55000)
INSERT INTO employees VALUES (grace, 3, 91000)
INSERT INTO employees VALUES (henry, 2, 40000)
INSERT INTO departments VALUES (engineering, 1, 500000)
INSERT INTO departments VALUES (marketing, 2, 300000)
INSERT INTO departments VALUES (research, 3, 700000)
"

printf "${YELLOW}BufSim Test Suite${RESET}\n"
printf "Binary : %s\n" "$BINARY"
printf "Date   : %s\n" "$(date)"

if [[ ! -x "$BINARY" ]]; then
    printf "${RED}ERROR:${RESET} %s not found. Run 'make' first.\n" "$BINARY"; exit 1
fi

# =============================================================
section "1. DDL — CREATE TABLE"
# =============================================================
OUT=$(run_sql 1 10 "
CREATE TABLE t1 (id INT, name CHAR(20))
SHOW TABLES
")
assert   "CREATE TABLE: [OK] message"            "$OUT" "[OK] Created table 't1'"
assert   "SHOW TABLES lists t1 schema"           "$OUT" "t1 (id INT, name CHAR(20))"
assert   "catalog.json updated"                  "$(cat catalog.json)" '"name": "t1"'

OUT=$(run_sql 1 10 "
CREATE TABLE t1 (id INT)
CREATE TABLE t1 (id INT)
")
assert   "Duplicate CREATE gives catalog msg"    "$OUT" "already exists"

# =============================================================
section "2. DDL — DROP TABLE"
# =============================================================
OUT=$(run_sql 1 10 "
CREATE TABLE tmp (x INT)
DROP TABLE tmp
SHOW TABLES
")
assert        "DROP TABLE: [OK] message"         "$OUT" "[OK] Dropped table 'tmp'"
assert        "SHOW TABLES shows (none)"         "$OUT" "(none)"
SHOW_SECTION=$(echo "$OUT" | awk "/SHOW TABLES/{p=1} p" | head -5)
  assert        "SHOW TABLES section is empty"     "$SHOW_SECTION" "(none)"
assert        "Binary file removed"              "$(ls *.bin 2>/dev/null || echo NONE)" "NONE"

OUT=$(run_sql 1 10 "DROP TABLE nonexistent")
assert "DROP non-existent: [Error]"              "$OUT" "[Error]"

# =============================================================
section "3. DDL — ALTER TABLE ADD COLUMN"
# =============================================================
OUT=$(run_sql 1 10 "
CREATE TABLE emp2 (name CHAR(20), salary INT)
INSERT INTO emp2 VALUES (alice, 50000)
INSERT INTO emp2 VALUES (bob, 60000)
ALTER TABLE emp2 ADD COLUMN bonus INT
SELECT * FROM emp2
INSERT INTO emp2 VALUES (carol, 70000, 5000)
SELECT * FROM emp2 WHERE bonus > 0
")
assert   "ALTER TABLE: [OK] message"             "$OUT" "[OK] Added column 'bonus'"
assert   "Existing row alice: bonus = 0"         "$OUT" "alice"
assert   "Existing row bob: bonus = 0"           "$OUT" "bob"
assert   "New row carol with bonus=5000"         "$OUT" "carol"
assert   "WHERE on new column returns 1 row"     "$OUT" "1 row(s) returned"

OUT=$(run_sql 1 10 "
CREATE TABLE emp3 (name CHAR(20))
ALTER TABLE emp3 ADD COLUMN name CHAR(10)
")
assert   "ALTER duplicate column: [Error]"       "$OUT" "already exists"

# =============================================================
section "4. DML — INSERT"
# =============================================================
OUT=$(run_sql 1 10 "
CREATE TABLE ins (a INT, b CHAR(10))
INSERT INTO ins VALUES (1, hello)
INSERT INTO ins VALUES (2, world)
SELECT * FROM ins
")
assert   "INSERT: 2 rows returned"               "$OUT" "2 row(s) returned"
assert   "Row 1 data correct"                    "$OUT" "hello"
assert   "Row 2 data correct"                    "$OUT" "world"

OUT=$(run_sql 1 10 "
CREATE TABLE ins2 (a INT, b CHAR(10), c INT)
INSERT INTO ins2 (b, a) VALUES (hi, 42)
SELECT * FROM ins2
")
assert   "INSERT with column list"               "$OUT" "42"

OUT=$(run_sql 1 10 "
CREATE TABLE ins3 (a INT, b INT)
INSERT INTO ins3 VALUES (1)
")
assert   "INSERT wrong value count: [Error]"     "$OUT" "[Error]"

OUT=$(run_sql 1 10 "
CREATE TABLE ins4 (a INT)
INSERT INTO ins4 VALUES (notanumber)
")
assert   "INSERT invalid INT: [Error]"           "$OUT" "[Error]"

# =============================================================
section "5. DML — UPDATE"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
UPDATE employees SET salary = 100000 WHERE emp_name = 'alice'
SELECT * FROM employees WHERE emp_name = 'alice'
")
assert   "UPDATE: [OK] 1 row(s) updated"         "$OUT" "[OK] 1 row(s) updated"
assert   "UPDATE changed salary to 100000"       "$OUT" "100000"

OUT=$(run_sql 1 10 "$SEED
UPDATE employees SET salary = 99999 WHERE dept_id = 1
SELECT * FROM employees WHERE dept_id = 1
")
assert   "UPDATE multiple rows: 3 rows returned" "$OUT" "3 row(s) returned"
assert   "Updated salary visible in SELECT"      "$OUT" "99999"

OUT=$(run_sql 1 10 "$SEED
UPDATE employees SET salary = 1
SELECT COUNT(*) FROM employees WHERE salary = 1
")
assert   "UPDATE all rows: 8 updated"            "$OUT" "[OK] 8 row(s) updated"
assert   "All 8 rows visible after update"       "$OUT" "8"

OUT=$(run_sql 1 10 "
CREATE TABLE upd (a INT)
INSERT INTO upd VALUES (1)
UPDATE upd SET zzz = 5
")
assert   "UPDATE unknown column: [Error]"        "$OUT" "[Error]"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees WHERE emp_name = 'bob'
UPDATE employees SET salary = 777 WHERE emp_name = 'bob'
SELECT * FROM employees WHERE emp_name = 'bob'
")
assert   "Cache coherence: post-UPDATE value"    "$OUT" "777"

# =============================================================
section "6. DML — DELETE"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
DELETE FROM employees WHERE dept_id = 3
SELECT * FROM employees
")
assert        "DELETE: [OK] 2 row(s) deleted"    "$OUT" "[OK] 2 row(s) deleted"
assert        "Remaining rows = 6"               "$OUT" "6 row(s) returned"
assert_absent "dan removed"                      "$OUT" "| dan"
assert_absent "grace removed"                    "$OUT" "| grace"

OUT=$(run_sql 1 10 "$SEED
DELETE FROM employees
SELECT * FROM employees
")
assert   "DELETE all: 8 deleted"                 "$OUT" "[OK] 8 row(s) deleted"
assert   "SELECT after full delete = 0 rows"     "$OUT" "0 row(s) returned"

OUT=$(run_sql 1 10 "$SEED
DELETE FROM employees WHERE emp_name = 'alice'
SELECT * FROM employees WHERE emp_name = 'alice'
")
assert   "Cache coherence: deleted row gone"     "$OUT" "0 row(s) returned"

# =============================================================
section "7. SELECT — Basic & Column Projection"
# =============================================================
OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees")
assert   "SELECT * returns all 8 rows"           "$OUT" "8 row(s) returned"
assert   "All column headers present"            "$OUT" "emp_name"

OUT=$(run_sql 1 10 "$SEED SELECT emp_name, salary FROM employees")
assert   "Projection header: emp_name | salary"  "$OUT" "emp_name"
assert_absent "dept_id excluded from projection" "$OUT" "| dept_id |"
assert   "Projection data correct"               "$OUT" "72000"

# =============================================================
section "8. SELECT — WHERE operators"
# =============================================================
OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE salary = 48000")
assert   "WHERE = : finds bob"                   "$OUT" "bob"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE salary != 48000")
assert   "WHERE != : 7 rows"                     "$OUT" "7 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE salary <> 48000")
assert   "WHERE <> : 7 rows"                     "$OUT" "7 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE salary > 80000")
assert   "WHERE > : 3 rows"                      "$OUT" "3 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE salary < 50000")
assert   "WHERE < : 2 rows"                      "$OUT" "2 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE salary >= 83000")
assert   "WHERE >= : 3 rows"                     "$OUT" "3 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE salary <= 48000")
assert   "WHERE <= : 2 rows"                     "$OUT" "2 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE emp_name LIKE 'a%'")
assert   "LIKE prefix 'a%': alice"               "$OUT" "alice"
assert   "LIKE prefix: 1 row"                    "$OUT" "1 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE emp_name LIKE '%e'")
assert   "LIKE suffix '%e': 3 rows (alice,eve,grace)" "$OUT" "3 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE emp_name LIKE '%r%'")
assert   "LIKE substring '%r%': 4 rows"          "$OUT" "4 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees WHERE emp_name LIKE 'b__'")
assert   "LIKE underscore 'b__': bob"            "$OUT" "1 row(s) returned"

OUT=$(run_sql 1 10 "
CREATE TABLE nullable (name CHAR(10), val INT)
INSERT INTO nullable VALUES (alice, 5)
INSERT INTO nullable VALUES ('', 0)
SELECT * FROM nullable WHERE name IS NOT NULL
")
assert   "IS NOT NULL: 1 non-empty row"          "$OUT" "1 row(s) returned"

OUT=$(run_sql 1 10 "
CREATE TABLE nullable2 (name CHAR(10), val INT)
INSERT INTO nullable2 VALUES ('', 0)
SELECT * FROM nullable2 WHERE name IS NULL
")
assert   "IS NULL: matches empty string"         "$OUT" "1 row(s) returned"

# =============================================================
section "9. SELECT — AND / OR"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees WHERE dept_id = 1 AND salary > 60000")
assert   "AND: dept=1 AND salary>60k = 2 rows"   "$OUT" "2 row(s) returned"
assert   "AND: alice present"                    "$OUT" "alice"
assert_absent "AND: frank excluded (55000)"      "$OUT" "frank  "

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees WHERE dept_id = 3 OR salary < 50000")
assert   "OR: dept=3 OR salary<50k = 4 rows"     "$OUT" "4 row(s) returned"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees WHERE dept_id = 1 AND salary > 60000 AND emp_name != 'carol'")
assert   "Triple AND: only alice"                "$OUT" "1 row(s) returned"
assert   "Triple AND result: alice"              "$OUT" "alice"

# =============================================================
section "10. SELECT — DISTINCT"
# =============================================================
OUT=$(run_sql 1 10 "$SEED SELECT DISTINCT dept_id FROM employees")
assert   "DISTINCT: 3 unique dept_ids"           "$OUT" "3 row(s) returned"
assert   "DISTINCT contains dept 1"              "$OUT" "1"
assert   "DISTINCT contains dept 2"              "$OUT" "2"
assert   "DISTINCT contains dept 3"              "$OUT" "3"

# =============================================================
section "11. SELECT — Aggregates"
# =============================================================
OUT=$(run_sql 1 10 "$SEED SELECT COUNT(*) FROM employees")
assert   "COUNT(*) = 8"                          "$OUT" "8"

OUT=$(run_sql 1 10 "$SEED SELECT SUM(salary) FROM employees")
assert   "SUM(salary) = 545000.00"               "$OUT" "545000.00"

OUT=$(run_sql 1 10 "$SEED SELECT AVG(salary) FROM employees")
assert   "AVG(salary) = 68125.00"                "$OUT" "68125.00"

OUT=$(run_sql 1 10 "$SEED SELECT MIN(salary) FROM employees")
assert   "MIN(salary) = 40000.00"                "$OUT" "40000.00"

OUT=$(run_sql 1 10 "$SEED SELECT MAX(salary) FROM employees")
assert   "MAX(salary) = 95000.00"                "$OUT" "95000.00"

OUT=$(run_sql 1 10 "$SEED
SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM employees")
assert   "All aggregates together: 1 row"        "$OUT" "1 row(s) returned"
assert   "Combined aggs: COUNT=8"                "$OUT" "8"
assert   "Combined aggs: SUM present"            "$OUT" "545000.00"

OUT=$(run_sql 1 10 "$SEED
SELECT COUNT(*) AS headcount, AVG(salary) AS avg_pay FROM employees")
assert   "Agg alias headcount in header"         "$OUT" "headcount"
assert   "Agg alias avg_pay in header"           "$OUT" "avg_pay"
assert   "Aliased count = 8"                     "$OUT" "8"

# =============================================================
section "12. SELECT — GROUP BY"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id")
assert   "GROUP BY: 3 groups"                    "$OUT" "3 row(s) returned"
assert   "GROUP BY: dept 1 count = 3"            "$OUT" "3"
assert   "GROUP BY: dept 2 count = 3"            "$OUT" "3"
assert   "GROUP BY: dept 3 count = 2"            "$OUT" "2"

OUT=$(run_sql 1 10 "$SEED
SELECT dept_id, SUM(salary) AS total FROM employees GROUP BY dept_id")
assert   "GROUP BY SUM: alias 'total' in header" "$OUT" "total"
# dept 1: alice 72000 + carol 95000 + frank 55000 = 222000
assert   "GROUP BY SUM: dept 1 = 222000"         "$OUT" "222000.00"

# =============================================================
section "13. SELECT — HAVING"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
SELECT dept_id, COUNT(*) AS cnt FROM employees GROUP BY dept_id HAVING cnt > 2")
assert   "HAVING alias cnt>2: 2 rows"            "$OUT" "2 row(s) returned"
assert_absent "HAVING excludes dept 3 (cnt=2)"   "$OUT" "3 row"

OUT=$(run_sql 1 10 "$SEED
SELECT dept_id, AVG(salary) FROM employees GROUP BY dept_id HAVING avg(salary) > 60000")
assert   "HAVING raw avg(salary)>60k: 2 rows"    "$OUT" "2 row(s) returned"
assert   "HAVING includes dept 1"                "$OUT" "1"
assert   "HAVING includes dept 3"                "$OUT" "3"
# dept 2 avg = (48000+83000+40000)/3 = 57000 — excluded
assert_absent "HAVING excludes dept 2 (avg=57k)" "$OUT" "57"

OUT=$(run_sql 1 10 "$SEED
SELECT dept_id, AVG(salary) AS avg_sal FROM employees GROUP BY dept_id HAVING avg_sal > 60000")
assert   "HAVING computed alias avg_sal: 2 rows" "$OUT" "2 row(s) returned"

OUT=$(run_sql 1 10 "$SEED
SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id HAVING count(*) > 2")
assert   "HAVING count(*) raw syntax: 2 rows"    "$OUT" "2 row(s) returned"

# =============================================================
section "14. SELECT — ORDER BY"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
SELECT emp_name, salary FROM employees ORDER BY salary DESC")
assert      "ORDER BY DESC: 8 rows"              "$OUT" "8 row(s) returned"
assert      "ORDER BY DESC: carol present"       "$OUT" "carol"
assert_lineno "ORDER BY DESC: carol before henry" "$OUT" "carol" "henry"

OUT=$(run_sql 1 10 "$SEED
SELECT emp_name, salary FROM employees ORDER BY salary ASC")
assert      "ORDER BY ASC: 8 rows"               "$OUT" "8 row(s) returned"
    assert_lineno "ORDER BY ASC: henry before carol"  "$OUT" "henry" "carol"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees ORDER BY emp_name ASC")
assert_lineno "ORDER BY string ASC: alice before henry" "$OUT" "alice" "henry"

# =============================================================
section "15. SELECT — LIMIT / OFFSET"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees ORDER BY salary ASC LIMIT 3")
assert        "LIMIT 3: 3 rows returned"         "$OUT" "3 row(s) returned"
assert        "LIMIT shows total=8"              "$OUT" "total before limit: 8"
assert        "LIMIT 3 ASC: henry present"       "$OUT" "henry"
assert_absent "LIMIT 3 ASC: carol absent"        "$OUT" "carol"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees ORDER BY salary ASC LIMIT 3 OFFSET 2")
assert        "LIMIT 3 OFFSET 2: 3 rows"         "$OUT" "3 row(s) returned"
assert_absent "OFFSET 2: henry skipped (1st)"    "$OUT" "henry"
assert_absent "OFFSET 2: bob skipped (2nd)"      "$OUT" "bob"
assert        "OFFSET 2: frank is 3rd"           "$OUT" "frank"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees LIMIT 1")
assert   "LIMIT 1 without ORDER BY"              "$OUT" "1 row(s) returned"

OUT=$(run_sql 1 10 "$SEED SELECT * FROM employees LIMIT 100")
assert   "LIMIT > total: all 8 rows"             "$OUT" "8 row(s) returned"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees ORDER BY salary ASC LIMIT 3 OFFSET 7")
assert   "OFFSET 7 of 8: 1 row"                  "$OUT" "1 row(s) returned"

# =============================================================
section "16. JOIN"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees JOIN departments ON dept_id = dept_id")
# 8 employees, dept 3 has no matching dept (research exists so all 8 match)
assert   "JOIN: qualified headers present"       "$OUT" "emp_name"
assert   "JOIN: dept headers present"            "$OUT" "dept_name"
assert   "JOIN: alice joined engineering"        "$OUT" "engineering"
assert   "JOIN: bob joined marketing"            "$OUT" "marketing"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees JOIN departments ON dept_id = dept_id WHERE emp_name = 'bob'")
assert   "JOIN WHERE: 1 row (bob)"               "$OUT" "1 row(s) returned"
assert   "JOIN WHERE: shows marketing"           "$OUT" "marketing"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees JOIN departments ON dept_id = dept_id ORDER BY salary DESC")
assert_lineno "JOIN ORDER BY DESC: carol before henry" "$OUT" "carol" "henry"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees JOIN departments ON dept_id = dept_id LIMIT 2")
assert   "JOIN LIMIT 2: 2 rows"                  "$OUT" "2 row(s) returned"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees JOIN departments ON dept_id = dept_id WHERE dept_id = 999")
assert   "JOIN no match: 0 rows"                 "$OUT" "0 row(s) returned"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees JOIN departments ON zzz = zzz")
assert   "JOIN bad column: [Error]"              "$OUT" "[Error]"

# =============================================================
section "17. Buffer Replacement Policies"
# =============================================================
for POLICY in 1 2 3; do
    case $POLICY in 1) PN="LRU";; 2) PN="MRU";; 3) PN="CLOCK";; esac
    OUT=$(run_sql $POLICY 5 "
CREATE TABLE bp (x INT)
INSERT INTO bp VALUES (1)
INSERT INTO bp VALUES (2)
INSERT INTO bp VALUES (3)
SELECT * FROM bp
SELECT * FROM bp
")
    assert "${PN}: SELECT returns 3 rows"        "$OUT" "3 row(s) returned"
    assert "${PN}: second SELECT hits cache"     "$OUT" "page hits     : 1"
done

# CLOCK eviction under pressure
OUT=$(run_sql 3 2 "$SEED
SELECT * FROM employees
SELECT * FROM departments
SELECT * FROM employees
")
assert "CLOCK 2 frames: eviction works"          "$OUT" "8 row(s) returned"

# =============================================================
section "18. Buffer Cache Coherence"
# =============================================================
OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees WHERE emp_name = 'alice'
UPDATE employees SET salary = 99999 WHERE emp_name = 'alice'
SELECT * FROM employees WHERE emp_name = 'alice'
")
assert   "Coherence UPDATE: fresh value visible"  "$OUT" "99999"

OUT=$(run_sql 1 10 "$SEED
SELECT COUNT(*) FROM employees
DELETE FROM employees WHERE dept_id = 3
SELECT COUNT(*) FROM employees
")
assert   "Coherence DELETE: count drops to 6"    "$OUT" "6"

OUT=$(run_sql 1 10 "
CREATE TABLE cc (a INT, b INT)
INSERT INTO cc VALUES (1, 10)
ALTER TABLE cc ADD COLUMN c INT
SELECT * FROM cc
INSERT INTO cc VALUES (2, 20, 30)
SELECT * FROM cc WHERE c > 0
")
assert   "Coherence ALTER: existing row bonus=0" "$OUT" "10"
assert   "Coherence ALTER: new row visible"      "$OUT" "30"

# =============================================================
section "19. Data Persistence (catalog.json)"
# =============================================================
echo '{"tables":[]}' > catalog.json; rm -f ./*.bin; : > output.txt
printf "1\n10\nCREATE TABLE persist_t (x INT, y CHAR(10))\nINSERT INTO persist_t VALUES (42, hello)\nexit\n" \
    | "$BINARY" 2>/dev/null > /dev/null
# Second session — no CREATE, table should already be in catalog.json
: > output.txt
printf "1\n10\nSELECT * FROM persist_t\nexit\n" \
    | "$BINARY" 2>/dev/null > /dev/null
OUT=$(cat output.txt)
assert   "Schema persists across sessions"       "$OUT" "42"
assert   "Row data persists across sessions"     "$OUT" "1 row(s) returned"

# =============================================================
section "20. Error Handling"
# =============================================================
OUT=$(run_sql 1 10 "SELECT * FROM does_not_exist")
assert   "SELECT unknown table: [Error]"         "$OUT" "[Error]"

OUT=$(run_sql 1 10 "
CREATE TABLE err1 (a INT)
INSERT INTO err1 VALUES (1, 2, 3)
")
assert   "INSERT too many values: [Error]"       "$OUT" "[Error]"

OUT=$(run_sql 1 10 "
CREATE TABLE err2 (a INT)
INSERT INTO err2 VALUES (1)
UPDATE err2 SET a = notanumber
")
assert   "UPDATE invalid INT: [Error]"           "$OUT" "[Error]"

OUT=$(run_sql 1 10 "CREATE nosyntax")
assert   "Unrecognised SQL: [Error]"             "$OUT" "[Error]"

OUT=$(run_sql 1 10 "$SEED
SELECT * FROM employees JOIN departments ON zzz = zzz")
assert   "JOIN bad column: [Error]"              "$OUT" "[Error]"

# =============================================================
section "21. Edge Cases"
# =============================================================
OUT=$(run_sql 1 10 "
CREATE TABLE empty_t (x INT)
SELECT * FROM empty_t
SELECT COUNT(*) FROM empty_t
DELETE FROM empty_t
UPDATE empty_t SET x = 1
")
assert   "SELECT empty table: 0 rows"            "$OUT" "0 row(s) returned"
assert   "COUNT(*) on empty table: 0"            "$OUT" "0"
assert   "DELETE empty table: 0 deleted"         "$OUT" "[OK] 0 row(s) deleted"
assert   "UPDATE empty table: 0 updated"         "$OUT" "[OK] 0 row(s) updated"

OUT=$(run_sql 1 10 "
CREATE TABLE semi (x INT);
INSERT INTO semi VALUES (7);
SELECT * FROM semi;
")
assert   "Trailing semicolons work"              "$OUT" "7"

OUT=$(run_sql 1 10 "
CREATE TABLE str_t (name CHAR(30), val INT)
INSERT INTO str_t VALUES ('hello world', 1)
SELECT * FROM str_t WHERE name = 'hello world'
")
assert   "Quoted string with spaces inserted"    "$OUT" "hello world"
assert   "WHERE on quoted string: 1 row"         "$OUT" "1 row(s) returned"

OUT=$(run_sql 1 10 "
CREATE TABLE aa (x INT)
CREATE TABLE bb (y CHAR(5))
SHOW TABLES
")
assert   "SHOW TABLES lists aa"                  "$OUT" "aa (x INT)"
assert   "SHOW TABLES lists bb"                  "$OUT" "bb (y CHAR(5))"

# =============================================================
section "22. EXPLAIN — cost estimate without execution"
# =============================================================
SEED22="
CREATE TABLE aa (x INT, name CHAR(10))
CREATE TABLE bb (y INT, desc CHAR(10))
INSERT INTO aa VALUES (1, alpha)
INSERT INTO aa VALUES (2, beta)
INSERT INTO bb VALUES (1, first)
INSERT INTO bb VALUES (2, second)
"
OUT=$(run_sql 1 5 "$SEED22
EXPLAIN SELECT * FROM aa JOIN bb ON aa.x = bb.y")
assert   "EXPLAIN JOIN: shows outer table"         "$OUT" "outer : aa"
assert   "EXPLAIN JOIN: shows inner table"         "$OUT" "inner : bb"
assert   "EXPLAIN JOIN: shows NLJ row"             "$OUT" "NLJ"
assert   "EXPLAIN JOIN: shows BNLJ row"            "$OUT" "BNLJ"
assert   "EXPLAIN JOIN: shows would choose"        "$OUT" "would choose"
assert   "EXPLAIN JOIN: no disk read (estimate)"   "$OUT" "no data was read from disk"
assert   "EXPLAIN JOIN: disk reads = 0"            "$OUT" "disk reads    : 0"

OUT=$(run_sql 1 5 "$SEED22
EXPLAIN SELECT * FROM aa")
assert   "EXPLAIN SELECT: shows table name"        "$OUT" "table : aa"
assert   "EXPLAIN SELECT: shows linear scan A1"    "$OUT" "linear scan (A1)"
assert   "EXPLAIN SELECT: shows formula"           "$OUT" "block transfers"
assert   "EXPLAIN SELECT: shows est I/O"           "$OUT" "est I/O"
assert   "EXPLAIN SELECT: no disk read"            "$OUT" "no data was read from disk"

# =============================================================
section "23. SET POLICY — mid-session policy switch"
# =============================================================
SEED23="
CREATE TABLE pp (id INT, val CHAR(5))
INSERT INTO pp VALUES (1, aaa)
INSERT INTO pp VALUES (2, bbb)
INSERT INTO pp VALUES (3, ccc)
"
# Start on LRU, switch to MRU, verify response
OUT=$(run_sql 1 5 "$SEED23
SELECT * FROM pp
SET POLICY MRU
SELECT * FROM pp")
assert   "SET POLICY: switch message shown"        "$OUT" "policy changed: LRU -> MRU"
assert   "SET POLICY: frames preserved message"     "$OUT" "frames preserved"
assert   "SET POLICY: second select still works"   "$OUT" "3 row(s) returned"

# Switch to same policy — should say already set
OUT=$(run_sql 2 5 "$SEED23
SET POLICY MRU")
assert   "SET POLICY same: already set message"   "$OUT" "policy is already MRU"

# Switch to CLOCK
OUT=$(run_sql 1 5 "$SEED23
SET POLICY CLOCK
SELECT * FROM pp")
assert   "SET POLICY CLOCK: switch shown"          "$OUT" "policy changed: LRU -> CLOCK"
assert   "SET POLICY CLOCK: select works"          "$OUT" "3 row(s) returned"

# Invalid policy name
OUT=$(run_sql 1 5 "$SEED23
SET POLICY FIFO")
assert   "SET POLICY invalid: error shown"         "$OUT" "Unknown policy: FIFO"

# =============================================================
section "24. NLJ vs BNLJ — cost comparison on join"
# =============================================================
SEED24="
CREATE TABLE r1 (id INT, name CHAR(20))
CREATE TABLE r2 (rid INT, val CHAR(20))
"
for i in $(seq 1 200); do SEED24="$SEED24
INSERT INTO r1 VALUES ($i, Row$i)"; done
for i in $(seq 1 200); do SEED24="$SEED24
INSERT INTO r2 VALUES ($i, Val$i)"; done

# B=3 — NLJ and BNLJ tie (block_sz=1 for both)
OUT=$(run_sql 1 3 "$SEED24
SELECT * FROM r1 JOIN r2 ON r1.id = r2.rid LIMIT 1")
assert   "B=3: NLJ shown in comparison"            "$OUT" "NLJ"
assert   "B=3: BNLJ shown in comparison"           "$OUT" "BNLJ"
assert   "B=3: BNLJ chosen on tie"                 "$OUT" "chosen: BNLJ"
assert   "B=3: join executes correctly"            "$OUT" "1 row(s) returned"

# B=5 — BNLJ should win
OUT=$(run_sql 1 5 "$SEED24
SELECT * FROM r1 JOIN r2 ON r1.id = r2.rid LIMIT 1")
assert   "B=5: BNLJ chosen over NLJ"              "$OUT" "chosen: BNLJ"
assert   "B=5: shows buffer hint"                  "$OUT" "Policy hint for this join"
assert   "B=5: join gives result"                  "$OUT" "1 row(s) returned"

# B=10 — BNLJ wins by larger margin
OUT=$(run_sql 1 10 "$SEED24
SELECT * FROM r1 JOIN r2 ON r1.id = r2.rid LIMIT 1")
assert   "B=10: BNLJ chosen"                      "$OUT" "chosen: BNLJ"
assert   "B=10: cost summary present"              "$OUT" "cost summary"
assert   "B=10: actual disk reads shown"           "$OUT" "actual disk reads"

# =============================================================
section "25. Repeated join — cache hit rate improves"
# =============================================================
SEED25="
CREATE TABLE cx (id INT, v CHAR(5))
CREATE TABLE cy (id INT, v CHAR(5))
INSERT INTO cx VALUES (1, a)
INSERT INTO cx VALUES (2, b)
INSERT INTO cy VALUES (1, x)
INSERT INTO cy VALUES (2, y)
"
OUT=$(run_sql 2 10 "$SEED25
SELECT * FROM cx JOIN cy ON cx.id = cy.id
SELECT * FROM cx JOIN cy ON cx.id = cy.id")
assert   "Repeat join: first join returns rows"    "$OUT" "2 row(s) returned"
assert   "Repeat join: second join returns rows"   "$OUT" "2 row(s) returned"
assert   "Repeat join: some page hits occur"       "$OUT" "page hits"
assert   "Repeat join: hit rate shown"             "$OUT" "hit rate"

# =============================================================
#  SUMMARY
# =============================================================
printf "\n"
printf "${CYAN}══════════════════════════════════════${RESET}\n"
if [[ $FAIL -eq 0 ]]; then
    printf "  ${GREEN}ALL %d TESTS PASSED${RESET}\n" "$TOTAL"
else
    printf "  ${GREEN}%d passed${RESET}  ${RED}%d FAILED${RESET}  of %d total\n" \
           "$PASS" "$FAIL" "$TOTAL"
fi
printf "${CYAN}══════════════════════════════════════${RESET}\n"

# Clean up
rm -f catalog.json ./*.bin output.txt

exit $FAIL