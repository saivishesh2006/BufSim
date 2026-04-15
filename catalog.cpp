#include "catalog.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstring>
#include <sys/stat.h>

// -------------------------------------------------------
// String helpers
// -------------------------------------------------------
static std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    size_t b = s.find_last_not_of (" \t\r\n");
    return (a == std::string::npos) ? "" : s.substr(a, b - a + 1);
}

static std::string extractStrVal(const std::string& line,
                                 const std::string& key) {
    std::string search = "\"" + key + "\"";
    size_t kp = line.find(search);
    if (kp == std::string::npos) return "";
    size_t colon = line.find(':', kp + search.size());
    if (colon == std::string::npos) return "";
    size_t q1 = line.find('"', colon + 1);
    if (q1 == std::string::npos) return "";
    size_t q2 = line.find('"', q1 + 1);
    if (q2 == std::string::npos) return "";
    return line.substr(q1 + 1, q2 - q1 - 1);
}

static int extractIntVal(const std::string& line, const std::string& key) {
    std::string search = "\"" + key + "\"";
    size_t kp = line.find(search);
    if (kp == std::string::npos) return -1;
    size_t colon = line.find(':', kp + search.size());
    if (colon == std::string::npos) return -1;
    size_t start = colon + 1;
    while (start < line.size() &&
           (line[start] == ' ' || line[start] == '\t')) start++;
    size_t end = start;
    while (end < line.size() &&
           (isdigit(line[end]) || line[end] == '-')) end++;
    return (start == end) ? -1 : std::stoi(line.substr(start, end - start));
}

static bool extractBoolVal(const std::string& line, const std::string& key) {
    std::string search = "\"" + key + "\"";
    size_t kp = line.find(search);
    if (kp == std::string::npos) return true; // default nullable=true
    size_t colon = line.find(':', kp + search.size());
    if (colon == std::string::npos) return true;
    size_t start = colon + 1;
    while (start < line.size() &&
           (line[start] == ' ' || line[start] == '\t')) start++;
    return line.substr(start, 4) != "fals"; // "false" → false, else true
}

// -------------------------------------------------------
// computeOffsets
// -------------------------------------------------------
void Catalog::computeOffsets(TableDef& table) {
    int offset = 0;
    for (auto& col : table.columns) {
        col.offset = offset;
        offset    += col.size;
    }
    table.record_size = offset;
}

// -------------------------------------------------------
// Constructor / load / save
// -------------------------------------------------------
Catalog::Catalog(const std::string& path) : catalog_file(path) { load(); }

void Catalog::save() {
    std::ofstream f(catalog_file);
    if (!f) { std::cerr << "[Catalog] Cannot write " << catalog_file << "\n"; return; }

    f << "{\n  \"tables\": [\n";
    bool first_t = true;
    for (auto& kv : tables) {
        const TableDef& td = kv.second;
        if (!first_t) f << ",\n";
        first_t = false;
        f << "    {\n";
        f << "      \"name\": \""     << td.name     << "\",\n";
        f << "      \"filename\": \"" << td.filename << "\",\n";
        f << "      \"columns\": [\n";
        bool first_c = true;
        for (auto& col : td.columns) {
            if (!first_c) f << ",\n";
            first_c = false;
            f << "        { \"name\": \""  << col.name
              << "\", \"type\": \""        << col.type
              << "\", \"size\": "          << col.size
              << ", \"nullable\": "        << (col.nullable ? "true" : "false")
              << " }";
        }
        f << "\n      ]\n    }";
    }
    f << "\n  ]\n}\n";
}

void Catalog::load() {
    std::ifstream f(catalog_file);
    if (!f) return;

    std::string content((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
    f.close();

    std::istringstream ss(content);
    std::string line;
    std::vector<std::string> lines;
    while (std::getline(ss, line)) lines.push_back(line);

    TableDef cur_table;
    bool in_table   = false;
    bool in_columns = false;

    for (auto& l_raw : lines) {
        std::string l = trim(l_raw);

        if (!in_table && l.find("\"name\"") != std::string::npos
                      && l.find("\"type\"") == std::string::npos) {
            cur_table = TableDef{};
            cur_table.name = extractStrVal(l, "name");
            in_table = true;
            continue;
        }
        if (in_table && !in_columns &&
            l.find("\"filename\"") != std::string::npos) {
            cur_table.filename = extractStrVal(l, "filename");
            continue;
        }
        if (in_table && l.find("\"columns\"") != std::string::npos) {
            in_columns = true;
            continue;
        }
        if (in_columns && l.find("\"name\"") != std::string::npos
                       && l.find("\"type\"") != std::string::npos) {
            ColumnDef col;
            col.name     = extractStrVal(l, "name");
            col.type     = extractStrVal(l, "type");
            col.size     = extractIntVal(l, "size");
            col.nullable = extractBoolVal(l, "nullable");
            col.offset   = 0;
            cur_table.columns.push_back(col);
            continue;
        }
        if (in_columns && l == "]") { in_columns = false; continue; }
        if (in_table && (l == "}" || l == "},") && !in_columns) {
            if (!cur_table.name.empty()) {
                computeOffsets(cur_table);
                tables[cur_table.name] = cur_table;
            }
            in_table = false;
        }
    }
}

// -------------------------------------------------------
// createTable
// -------------------------------------------------------
bool Catalog::createTable(const std::string& name,
                          const std::vector<ColumnDef>& cols) {
    if (tables.count(name)) {
        std::cerr << "[Catalog] Table '" << name << "' already exists.\n";
        return false;
    }
    TableDef td;
    td.name     = name;
    td.filename = "data/" + name + ".bin";  // ← also change this
    td.columns  = cols;
    computeOffsets(td);
    mkdir("data", 0755);
    tables[name] = td;
    save();
    return true;
}

// -------------------------------------------------------
// dropTable
// -------------------------------------------------------
bool Catalog::dropTable(const std::string& name) {
    auto it = tables.find(name);
    if (it == tables.end()) return false;
    tables.erase(it);
    save();
    return true;
}

// -------------------------------------------------------
// addColumn — appends a column to an existing table schema.
// NOTE: existing rows are NOT migrated here; the executor
// rewrites the binary file with the new layout.
// -------------------------------------------------------
bool Catalog::addColumn(const std::string& table_name,
                        const ColumnDef& col) {
    auto it = tables.find(table_name);
    if (it == tables.end()) return false;
    TableDef& td = it->second;
    // Prevent duplicate column names
    for (auto& c : td.columns)
        if (c.name == col.name) return false;
    td.columns.push_back(col);
    computeOffsets(td);
    save();
    return true;
}

// -------------------------------------------------------
// Accessors
// -------------------------------------------------------
bool Catalog::hasTable(const std::string& name) const {
    return tables.count(name) > 0;
}

const TableDef& Catalog::getTable(const std::string& name) const {
    return tables.at(name);
}

TableDef& Catalog::getTableMut(const std::string& name) {
    return tables.at(name);
}

std::vector<std::string> Catalog::allTables() const {
    std::vector<std::string> v;
    for (auto& kv : tables) v.push_back(kv.first);
    return v;
}
