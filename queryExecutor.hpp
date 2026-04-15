#ifndef __QUERY_EXECUTOR_H_
#define __QUERY_EXECUTOR_H_

#include "bufferManager.hpp"
#include "catalog.hpp"
#include "sqlParser.hpp"
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>

#define LRU   1
#define MRU   2
#define CLOCK 3

struct Row { std::vector<std::string> cols; };

struct SessionStats {
    int  queries=0; long accesses=0, diskReads=0, pageHits=0, pageMisses=0;
};

class QueryExecutor {
private:
    std::vector<std::pair<std::string,ReplacementPolicy*>> bufManagers;
    int num_frames, policy;
    Catalog& catalog;
    std::ofstream& out;
    std::unordered_map<std::string,FILE*> readHandles;
    std::unordered_map<std::string,int>   tableAccessCount;
    SessionStats sessionStats;

    int  getNumPages(FILE* fp);
    void tee(const std::string& s);
    void printPoolState(const std::string& label);
    void printBufferHint(int np_outer, int np_inner);
    long costNLJ (int M,int N);
    long costBNLJ(int M,int N,int B);
    long costSelect(int M);
    void printHint(const std::string& query_type, const std::string& table1);

    FILE*       getReadHandle(const std::string& tname);
    std::string readField(const char* page,int base_offset,const ColumnDef& col);
    bool evalCondition(const std::string& fv,const std::string& op,const std::string& cv,const std::string& type);
    bool matchesWhere(const char* page,int rec_offset,const TableDef& tdef,const WhereClause& wc);
    bool likeMatch(const std::string& text,const std::string& pattern);

    void printHeader(const std::vector<std::string>& cols,const std::vector<int>& widths);
    void printRow(const Row& row,const std::vector<int>& widths);
    void printSep(const std::vector<int>& widths);
    void writeStats(const std::string& label);
    std::vector<int> computeWidths(const std::vector<std::string>& headers,const std::vector<Row>& rows);
    Row projectRow(const char* page,int rec_offset,const TableDef& tdef,const std::vector<SelectCol>& scols);

    void execCreate(const ParsedQuery& pq);
    void execDrop(const ParsedQuery& pq);
    void execAlterAdd(const ParsedQuery& pq);
    void execInsert(const ParsedQuery& pq);
    void execSelect(const ParsedQuery& pq);
    void execJoin(const ParsedQuery& pq);
    void execUpdate(const ParsedQuery& pq);
    void execDelete(const ParsedQuery& pq);
    void execExplain(const ParsedQuery& pq);
    void execSetPolicy(const ParsedQuery& pq);

public:
    QueryExecutor(int num_frames,int policy,Catalog& cat,std::ofstream& output);
    ~QueryExecutor();
    void execute(const ParsedQuery& pq);
    void printSessionSummary();
};

#endif