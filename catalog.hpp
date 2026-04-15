#ifndef __CATALOG_H_
#define __CATALOG_H_

#include <string>
#include <vector>
#include <unordered_map>

// -------------------------------------------------------
// ColumnDef
// -------------------------------------------------------
struct ColumnDef {
    std::string name;
    std::string type;   // "INT" or "CHAR"
    int size;           // bytes: 4 for INT, N for CHAR(N)
    int offset;         // byte offset within a record (computed by catalog)
    bool nullable;      // supports IS NULL / IS NOT NULL
};

// -------------------------------------------------------
// TableDef
// -------------------------------------------------------
struct TableDef {
    std::string name;
    std::string filename;
    int record_size;
    std::vector<ColumnDef> columns;
};

// -------------------------------------------------------
// Catalog
// -------------------------------------------------------
class Catalog {
private:
    std::unordered_map<std::string, TableDef> tables;
    std::string catalog_file;
    void computeOffsets(TableDef& table);

public:
    explicit Catalog(const std::string& catalog_path = "catalog.json");
    void save();
    void load();

    bool createTable(const std::string& name,
                     const std::vector<ColumnDef>& cols);
    bool dropTable  (const std::string& name);
    bool addColumn  (const std::string& table_name,
                     const ColumnDef& col);

    bool hasTable(const std::string& name) const;
    const TableDef& getTable(const std::string& name) const;
    TableDef&       getTableMut(const std::string& name);
    std::vector<std::string> allTables() const;
};

#endif // __CATALOG_H_
