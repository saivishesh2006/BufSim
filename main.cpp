#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include "catalog.hpp"
#include "sqlParser.hpp"
#include "queryExecutor.hpp"

using namespace std;

void printBanner() {
    cout << "\n";
    cout << "=======================================================\n";
    cout << "          BufSim - SQL Query Engine                    \n";
    cout << "=======================================================\n";
    cout << "Supported SQL:\n";
    cout << "  CREATE TABLE t (col INT, col CHAR(N), ...)\n";
    cout << "  DROP TABLE t\n";
    cout << "  ALTER TABLE t ADD [COLUMN] col TYPE\n";
    cout << "  INSERT INTO t [(col,...)] VALUES (v1,...)\n";
    cout << "  SELECT [DISTINCT] * | col [AS a], COUNT(*), SUM/AVG/MIN/MAX(col)\n";
    cout << "    FROM t\n";
    cout << "    [WHERE col op val [AND|OR ...]]\n";
    cout << "    [GROUP BY col] [HAVING col op val]\n";
    cout << "    [ORDER BY col [ASC|DESC]]\n";
    cout << "    [LIMIT n [OFFSET m]]\n";
    cout << "  SELECT ... FROM t1 JOIN t2 ON t1.c = t2.c [WHERE ...]\n";
    cout << "  UPDATE t SET col=val [, ...] [WHERE ...]\n";
    cout << "  DELETE FROM t [WHERE ...]\n";
    cout << "  EXPLAIN SELECT * FROM t\n";
    cout << "  EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.c = t2.c\n";
    cout << "  SET POLICY LRU | MRU | CLOCK\n";
    cout << "  SHOW TABLES\n";
    cout << "  exit\n";
    cout << "  Operators: = != <> > < >= <= LIKE IS NULL IS NOT NULL\n";
    cout << "=======================================================\n";
    cout << "Results written to: output.txt\n\n";
}

int choosePolicyInteractive() {
    cout << "Select Buffer Replacement Policy:\n";
    cout << "  1: LRU   (Least Recently Used)\n";
    cout << "  2: MRU   (Most Recently Used)\n";
    cout << "  3: CLOCK (Second Chance)\n";
    cout << "Choice: ";
    int c = 1;
    if (!(cin >> c) || c < 1 || c > 3) {
        cout << "Invalid, defaulting to LRU\n";
        c = LRU;
        cin.clear();
    }
    return c;
}

int chooseFramesInteractive() {
    cout << "Enter number of buffer frames (e.g. 10): ";
    int f = 10;
    if (!(cin >> f) || f < 3) {
        cout << "Minimum 3 frames, using 3\n";
        f = 3;
        cin.clear();
    }
    return f;
}

string policyName(int p) {
    if (p == LRU)   return "LRU";
    if (p == MRU)   return "MRU";
    if (p == CLOCK) return "CLOCK";
    return "?";
}

int main(int /*argc*/, char* /*argv*/[]) {
    printBanner();

    int policy     = choosePolicyInteractive();
    int num_frames = chooseFramesInteractive();

    cout << "\nUsing " << policyName(policy)
         << " with " << num_frames << " frames.\n\n";

    ofstream out("output.txt", ios::app);
    if (!out) { cerr << "Cannot open output.txt\n"; return 1; }
    out << "\n========================================\n";
    out << "Session: policy=" << policyName(policy)
        << " frames=" << num_frames << "\n";
    out << "========================================\n";

    Catalog       catalog("catalog.json");
    QueryExecutor executor(num_frames, policy, catalog, out);

    cin.ignore();
    string line;
    while (true) {
        cout << "SQL> ";
        if (!getline(cin, line)) break;

        size_t s = line.find_first_not_of(" \t");
        if (s == string::npos) continue;
        line = line.substr(s);

        size_t e = line.find_last_not_of(" \t\r\n");
        if (e != string::npos) line = line.substr(0, e + 1);
        if (line.empty()) continue;

        string uline = line;
        transform(uline.begin(), uline.end(), uline.begin(), ::toupper);

        if (uline == "EXIT" || uline == "QUIT") {
            cout << "Goodbye!\n";
            executor.printSessionSummary();
            break;
        }

        if (uline == "SHOW TABLES") {
            auto tables = catalog.allTables();
            sort(tables.begin(), tables.end());
            cout << "Tables:\n";
            out  << "\n--- SHOW TABLES ---\n";
            if (tables.empty()) {
                cout << "  (none)\n"; out << "  (none)\n";
            }
            for (auto& t : tables) {
                const TableDef& td = catalog.getTable(t);
                string schema = t + " (";
                for (size_t i = 0; i < td.columns.size(); ++i) {
                    if (i) schema += ", ";
                    schema += td.columns[i].name + " " + td.columns[i].type;
                    if (td.columns[i].type == "CHAR")
                        schema += "(" + to_string(td.columns[i].size) + ")";
                }
                schema += ")";
                cout << "  " << schema << "\n";
                out  << "  " << schema << "\n";
            }
            continue;
        }

        if (!line.empty() && line.back() == ';') line.pop_back();

        out << "\nSQL: " << line << "\n";
        cout.flush();

        ParsedQuery pq = SQLParser::parse(line);
        executor.execute(pq);
    }

    out.close();
    cout << "\nAll results written to output.txt\n";
    return 0;
}