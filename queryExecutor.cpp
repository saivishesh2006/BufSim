#include "queryExecutor.hpp"
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <cmath>
#include <map>
#include <set>

using namespace std;

// ============================================================
// Constructor / Destructor
// ============================================================
QueryExecutor::QueryExecutor(int nf, int pol, Catalog& cat, ofstream& output)
    : num_frames(nf), policy(pol), catalog(cat), out(output)
{
    if      (pol == LRU)   bufManagers.push_back({"LRU",   new LRUBufferManager(nf)});
    else if (pol == MRU)   bufManagers.push_back({"MRU",   new MRUBufferManager(nf)});
    else if (pol == CLOCK) bufManagers.push_back({"CLOCK", new ClockBufferManager(nf)});
    else { cerr << "Invalid policy\n"; exit(1); }
}

QueryExecutor::~QueryExecutor() {
    for (auto& kv : readHandles) if (kv.second) fclose(kv.second);
    for (auto& bm : bufManagers) delete bm.second;
}

// ============================================================
// getNumPages
// ============================================================
int QueryExecutor::getNumPages(FILE* fp) {
    if (!fp) return 0;
    long pos = ftell(fp);
    fseek(fp, 0, SEEK_END);
    int np = (int)(ftell(fp) / PAGE_SIZE);
    fseek(fp, pos, SEEK_SET);
    return np;
}

// ============================================================
// I/O cost formulas
// ============================================================
// NLJ: M_R + M_S * M_R  (one page at a time from outer, full inner scan per outer page)
long QueryExecutor::costNLJ(int M, int N) {
    return (long)M + (long)N * M;
}

// BNLJ: M_R + M_S * ceil(M_R / (B-2))
long QueryExecutor::costBNLJ(int M, int N, int B) {
    if (B < 3) B = 3;
    long blocks = (long)ceil((double)M / (B - 2));
    return (long)M + blocks * N;
}

long QueryExecutor::costSelect(int M) {
    return M;
}

// ============================================================
// Policy hint
// ============================================================
void QueryExecutor::printHint(const string& query_type, const string& table1) {
    string hint, reason;
    if (query_type == "JOIN") {
        hint   = "MRU";
        reason = "BNLJ repeatedly scans inner table — MRU evicts last-used inner page keeping older ones warm";
    } else if (query_type == "SELECT_REPEAT") {
        hint   = "LRU";
        reason = "table '" + table1 + "' accessed before — LRU retains pages for warm cache hits";
    } else {
        hint   = "MRU";
        reason = "sequential scan — MRU prevents buffer pollution from streaming reads";
    }
    string active = (policy==LRU)?"LRU":(policy==MRU)?"MRU":"CLOCK";
    string match  = (active == hint) ? " [ACTIVE POLICY MATCHES]" : "";
    tee("[Hint] Recommended: " + hint + match + " | " + reason + "\n");
}

// ============================================================
// getReadHandle
// ============================================================
FILE* QueryExecutor::getReadHandle(const string& tname) {
    auto it = readHandles.find(tname);
    if (it != readHandles.end()) return it->second;
    const TableDef& tdef = catalog.getTable(tname);
    FILE* fp = fopen(tdef.filename.c_str(), "rb");
    if (!fp) { tee("[Error] Cannot open " + tdef.filename + "\n"); return nullptr; }
    readHandles[tname] = fp;
    return fp;
}

// ============================================================
// readField
// ============================================================
string QueryExecutor::readField(const char* page, int base_offset, const ColumnDef& col) {
    if (col.type == "INT") {
        int val = 0;
        memcpy(&val, page + base_offset + col.offset, sizeof(int));
        return to_string(val);
    } else {
        int sz = min(col.size, 255);
        string s(page + base_offset + col.offset, sz);
        size_t end = s.find('\0');
        if (end != string::npos) s.resize(end);
        return s;
    }
}

// ============================================================
// likeMatch
// ============================================================
bool QueryExecutor::likeMatch(const string& text, const string& pattern) {
    int m = (int)text.size(), n = (int)pattern.size();
    vector<vector<bool>> dp(m+1, vector<bool>(n+1, false));
    dp[0][0] = true;
    for (int j = 1; j <= n; ++j)
        if (pattern[j-1] == '%') dp[0][j] = dp[0][j-1];
    for (int i = 1; i <= m; ++i)
        for (int j = 1; j <= n; ++j) {
            char pc = pattern[j-1];
            if (pc == '%')      dp[i][j] = dp[i-1][j] || dp[i][j-1];
            else if (pc == '_') dp[i][j] = dp[i-1][j-1];
            else                dp[i][j] = dp[i-1][j-1] && (tolower(text[i-1])==tolower(pc));
        }
    return dp[m][n];
}

// ============================================================
// evalCondition
// ============================================================
bool QueryExecutor::evalCondition(const string& fv, const string& op,
                                   const string& cv, const string& type) {
    if (op=="IS")     return fv.empty();
    if (op=="IS NOT") return !fv.empty();
    if (op=="LIKE")   return likeMatch(fv, cv);
    if (type=="INT") {
        long ia=0,ib=0;
        try{ia=stol(fv);}catch(...){}
        try{ib=stol(cv);}catch(...){}
        if(op=="=")  return ia==ib;
        if(op=="!=") return ia!=ib;
        if(op==">")  return ia>ib;
        if(op=="<")  return ia<ib;
        if(op==">=") return ia>=ib;
        if(op=="<=") return ia<=ib;
    } else {
        if(op=="=")  return fv==cv;
        if(op=="!=") return fv!=cv;
        if(op==">")  return fv>cv;
        if(op=="<")  return fv<cv;
        if(op==">=") return fv>=cv;
        if(op=="<=") return fv<=cv;
    }
    return false;
}

// ============================================================
// matchesWhere
// ============================================================
bool QueryExecutor::matchesWhere(const char* page, int rec_offset,
                                  const TableDef& tdef, const WhereClause& wc) {
    if (wc.isEmpty()) return true;
    bool result = false;
    {
        const Condition& c = wc.conditions[0];
        string fv, ct="CHAR";
        for (auto& col : tdef.columns)
            if (col.name==c.col1) { fv=readField(page,rec_offset,col); ct=col.type; break; }
        result = evalCondition(fv, c.op, c.value, ct);
    }
    for (size_t ci = 0; ci < wc.connectors.size(); ++ci) {
        const Condition& c = wc.conditions[ci+1];
        string fv, ct="CHAR";
        for (auto& col : tdef.columns)
            if (col.name==c.col1) { fv=readField(page,rec_offset,col); ct=col.type; break; }
        bool next = evalCondition(fv, c.op, c.value, ct);
        if (wc.connectors[ci]=="AND") result = result && next;
        else                          result = result || next;
    }
    return result;
}

// ============================================================
// projectRow
// ============================================================
Row QueryExecutor::projectRow(const char* page, int rec_offset,
                               const TableDef& tdef, const vector<SelectCol>& scols) {
    Row row;
    if (!scols.empty() && scols[0].star) {
        for (auto& col : tdef.columns) row.cols.push_back(readField(page,rec_offset,col));
        return row;
    }
    for (auto& sc : scols) {
        if (sc.star) {
            for (auto& col : tdef.columns) row.cols.push_back(readField(page,rec_offset,col));
        } else if (sc.agg == AggFunc::NONE) {
            bool found = false;
            for (auto& col : tdef.columns)
                if (col.name==sc.col) { row.cols.push_back(readField(page,rec_offset,col)); found=true; break; }
            if (!found) row.cols.push_back("NULL");
        } else {
            row.cols.push_back("");
        }
    }
    return row;
}

// ============================================================
// Output helpers
// ============================================================
vector<int> QueryExecutor::computeWidths(const vector<string>& headers, const vector<Row>& rows) {
    vector<int> w(headers.size(), 0);
    for (size_t i = 0; i < headers.size(); ++i) w[i] = (int)headers[i].size();
    for (auto& r : rows)
        for (size_t i = 0; i < r.cols.size() && i < w.size(); ++i)
            w[i] = max(w[i], (int)r.cols[i].size());
    return w;
}

void QueryExecutor::printSep(const vector<int>& widths) {
    string line = "";
    for (size_t i = 0; i < widths.size(); ++i) {
        line += " " + string(widths[i]+2, '-');
        if (i+1 < widths.size()) line += " ";
    }
    out  << line << "\n";
    cout << line << "\n";
}

void QueryExecutor::printHeader(const vector<string>& cols, const vector<int>& widths) {
    string row = "";
    for (size_t i = 0; i < cols.size(); ++i) {
        ostringstream cell; cell << left << setw(widths[i]) << cols[i];
        row += "  " + cell.str();
        if (i+1 < cols.size()) row += "  ";
    }
    out  << row << "\n";
    cout << row << "\n";
    printSep(widths);
}

void QueryExecutor::printRow(const Row& row, const vector<int>& widths) {
    string line = "";
    for (size_t i = 0; i < row.cols.size() && i < widths.size(); ++i) {
        ostringstream cell; cell << left << setw(widths[i]) << row.cols[i];
        line += "  " + cell.str();
        if (i+1 < widths.size()) line += "  ";
    }
    out  << line << "\n";
    cout << line << "\n";
}

// ============================================================
// tee — write same string to both output file and terminal
// ============================================================
void QueryExecutor::tee(const string& s) {
    out  << s;
    cout << s;
}

// ============================================================
// printPoolState — show each frame: table, page, recency, pins
// ============================================================
void QueryExecutor::printPoolState(const string& label) {
    // Build reverse map: FILE* -> table name
    unordered_map<FILE*, string> fp2name;
    for (auto& kv : readHandles) fp2name[kv.second] = kv.first;

    auto snap = bufManagers[0].second->getSnapshot();
    string pol = (policy==LRU)?"LRU":(policy==MRU)?"MRU":"CLOCK";

    tee("\nbuffer pool state  [" + label + "]\n");
    tee("  policy : " + pol + "  |  capacity : " + to_string(num_frames) + " frames\n");

    if (snap.empty()) {
        tee("  (buffer pool is empty — no pages loaded yet)\n");
        return;
    }

    // header
    tee("\n");
    tee("  slot  table            page  pin  ");
    if (policy == CLOCK) tee("2nd-chance  ");
    if (policy == LRU)   tee("recency (1=most recent, will evict last)  ");
    if (policy == MRU)   tee("recency (1=most recent, evict candidate)  ");
    tee("\n");
    tee("  ----  ---------------  ----  ---  ");
    if (policy == CLOCK) tee("----------  ");
    if (policy != CLOCK) tee("----------------------------------------  ");
    tee("\n");

    int slot = 1;
    for (auto& fi : snap) {
        if (!fi.occupied) { slot++; continue; }
        string tname = "?";
        if (fp2name.count(fi.fp)) tname = fp2name[fi.fp];

        ostringstream row;
        row << "  " << left << setw(4) << slot
            << "  " << left << setw(15) << tname
            << "  " << left << setw(4) << fi.page_num
            << "  " << left << setw(3) << fi.pin_count
            << "  ";

        if (policy == CLOCK) {
            row << left << setw(10) << (fi.second_chance ? "yes" : "no");
        } else {
            // show recency label
            string tag = "#" + to_string(fi.recency_rank);
            if (fi.recency_rank == 1) {
                if (policy == LRU) tag += " (most recent)";
                if (policy == MRU) tag += " (most recent - EVICT NEXT)";
            }
            if (fi.recency_rank == (int)snap.size()) {
                if (policy == LRU) tag += " (EVICT NEXT)";
                if (policy == MRU) tag += " (least recent)";
            }
            row << left << setw(40) << tag;
        }
        tee(row.str() + "\n");
        slot++;
    }

    int used  = (int)snap.size();
    int free_f = num_frames - used;
    tee("\n");
    tee("  used : " + to_string(used) +
        "  free : " + to_string(free_f) +
        "  total : " + to_string(num_frames) + "\n");
}

// ============================================================
// printBufferHint — buffer state + per-policy cost hint
// ============================================================
void QueryExecutor::printBufferHint(int np_outer, int np_inner) {
    string pol = (policy==LRU)?"LRU":(policy==MRU)?"MRU":"CLOCK";

    long c_nlj  = costNLJ (np_outer, np_inner);
    long c_bnlj = costBNLJ(np_outer, np_inner, num_frames);

    tee("\nPolicy hint for this join\n");
    tee("  All three policies use the same cost formula\n");
    tee("  but differ in which page gets evicted at runtime\n");
    tee("\n");
    tee("  LRU   NLJ=" + to_string(c_nlj) + "  BNLJ=" + to_string(c_bnlj) + "\n");
    tee("        evicts the least recently used page\n");
    tee("        good when the same pages are accessed again soon\n");
    tee("\n");
    tee("  MRU   NLJ=" + to_string(c_nlj) + "  BNLJ=" + to_string(c_bnlj) + "\n");
    tee("        evicts the most recently used page\n");
    tee("        best for join inner scans -- the just-used inner page\n");
    tee("        is evicted keeping older outer block pages warm\n");
    tee("\n");
    tee("  CLOCK NLJ=" + to_string(c_nlj) + "  BNLJ=" + to_string(c_bnlj) + "\n");
    tee("        second-chance eviction -- balanced between LRU and MRU\n");
    tee("        reasonable for mixed workloads\n");
    tee("\n");
    if      (pol == "LRU")   tee("  you are on LRU -- switch to MRU for better join performance\n");
    else if (pol == "MRU")   tee("  you are on MRU -- good choice for this join\n");
    else                     tee("  you are on CLOCK -- decent choice; MRU may do better on inner scan\n");
}

// ============================================================
// writeStats
// ============================================================
void QueryExecutor::writeStats(const string& label) {
    BufStats s = bufManagers[0].second->getStats();
    ostringstream hr;
    if (s.accesses > 0) hr << fixed << setprecision(2) << (100.0*s.pageHits/s.accesses) << "%";
    else hr << "N/A";

    tee("\nBuffer stats  [" + label + "]\n");
    tee("  page accesses : " + to_string(s.accesses)  + "\n");
    tee("  disk reads    : " + to_string(s.diskreads) + "\n");
    tee("  page hits     : " + to_string(s.pageHits)  + "\n");
    tee("  page misses   : " + to_string(s.accesses - s.pageHits) + "\n");
    tee("  hit rate      : " + hr.str() + "\n");

    sessionStats.queries++;
    sessionStats.accesses   += s.accesses;
    sessionStats.diskReads  += s.diskreads;
    sessionStats.pageHits   += s.pageHits;
    sessionStats.pageMisses += (s.accesses - s.pageHits);
}

// ============================================================
// printSessionSummary
// ============================================================
void QueryExecutor::printSessionSummary() {
    ostringstream hr;
    if (sessionStats.accesses > 0)
        hr << fixed << setprecision(2)
           << (100.0*sessionStats.pageHits/sessionStats.accesses) << "%";
    else hr << "N/A";

    tee("\n-- session summary --\n");
    tee("  queries run   : " + to_string(sessionStats.queries)    + "\n");
    tee("  page accesses : " + to_string(sessionStats.accesses)   + "\n");
    tee("  disk reads    : " + to_string(sessionStats.diskReads)  + "\n");
    tee("  page hits     : " + to_string(sessionStats.pageHits)   + "\n");
    tee("  page misses   : " + to_string(sessionStats.pageMisses) + "\n");
    tee("  hit rate      : " + hr.str() + "\n");
}

// ============================================================
// execute
// ============================================================
void QueryExecutor::execute(const ParsedQuery& pq) {
    if (!pq.error.empty()) {
        tee("[Error] " + pq.error + "\n");
        return;
    }
    for (auto& bm : bufManagers) bm.second->clearStats();
    switch (pq.type) {
        case QueryType::CREATE_TABLE:    execCreate   (pq); break;
        case QueryType::DROP_TABLE:      execDrop     (pq); break;
        case QueryType::ALTER_TABLE_ADD: execAlterAdd (pq); break;
        case QueryType::INSERT:          execInsert   (pq); break;
        case QueryType::SELECT:          execSelect   (pq); break;
        case QueryType::JOIN:            execJoin     (pq); break;
        case QueryType::UPDATE:          execUpdate   (pq); break;
        case QueryType::DELETE_FROM:     execDelete   (pq); break;
        case QueryType::EXPLAIN:         execExplain  (pq); break;
        case QueryType::SET_POLICY:      execSetPolicy(pq); break;
        default: tee("[Error] Unknown query type\n");
    }
}

// ============================================================
// execExplain — show cost estimates without running the query
// Usage: EXPLAIN SELECT * FROM t
//        EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.c = t2.c
// ============================================================
void QueryExecutor::execExplain(const ParsedQuery& pq) {
    string pol = (policy==LRU)?"LRU":(policy==MRU)?"MRU":"CLOCK";

    // show current pool state — EXPLAIN reads nothing
    printPoolState("current pool (EXPLAIN reads no pages)");

    if (pq.explain_type == "JOIN") {
        if (!catalog.hasTable(pq.table1) || !catalog.hasTable(pq.table2)) {
            tee("explain error: one or both tables not found\n"); return;
        }
        FILE* fp1 = getReadHandle(pq.table1);
        FILE* fp2 = getReadHandle(pq.table2);
        if (!fp1 || !fp2) return;
        int np1 = getNumPages(fp1), np2 = getNumPages(fp2);

        // smaller as outer
        string oname = pq.table1, iname = pq.table2;
        int np_o = np1, np_i = np2;
        if (np2 < np1) { swap(oname,iname); swap(np_o,np_i); }

        long c_nlj  = costNLJ (np_o, np_i);
        long c_bnlj = costBNLJ(np_o, np_i, num_frames);
        string chosen = (c_bnlj <= c_nlj) ? "BNLJ" : "NLJ";

        tee("\nexplain join\n");
        tee("  outer : " + oname + "  (" + to_string(np_o) + " pages)\n");
        tee("  inner : " + iname + "  (" + to_string(np_i) + " pages)\n");
        tee("  buffer: B = " + to_string(num_frames) + " frames\n");
        tee("  policy: " + pol + "\n");
        tee("\n");
        tee("  algorithm    formula                        est I/O\n");
        tee("  ---------    -------                        -------\n");
        {
            ostringstream f1, f2;
            f1 << np_o << " + " << np_i << " x " << np_o;
            f2 << np_o << " + " << np_i << " x ceil(" << np_o << "/" << max(1,num_frames-2) << ")";
            tee("  NLJ          " + f1.str() +
                string(max(1,(int)(31-f1.str().size())),' ') + to_string(c_nlj) + "\n");
            tee("  BNLJ         " + f2.str() +
                string(max(1,(int)(31-f2.str().size())),' ') + to_string(c_bnlj) + "\n");
        }
        tee("\n  would choose: " + chosen + "\n");
        tee("  (no data was read from disk — this is just the estimate)\n");

    } else {
        // SELECT
        if (!catalog.hasTable(pq.table_name)) {
            tee("explain error: table '" + pq.table_name + "' not found\n"); return;
        }
        FILE* fp = getReadHandle(pq.table_name);
        if (!fp) return;
        int np = getNumPages(fp);
        long c = costSelect(np);

        tee("\nexplain select\n");
        tee("  table : " + pq.table_name + "  (" + to_string(np) + " pages)\n");
        tee("  policy: " + pol + "\n");
        tee("  algorithm : linear scan (A1)\n");
        tee("  formula   : b_r = " + to_string(np) + " block transfers + 1 seek\n");
        tee("  est I/O   : " + to_string(c) + " page reads\n");
        tee("  (no data was read from disk — this is just the estimate)\n");
    }
}

// ============================================================
// execSetPolicy — switch buffer replacement policy mid-session
// Usage: SET POLICY LRU | MRU | CLOCK
// ============================================================
void QueryExecutor::execSetPolicy(const ParsedQuery& pq) {
    if (pq.new_policy < 1 || pq.new_policy > 3) {
        tee("error: invalid policy\n"); return;
    }
    string old_pol = (policy==LRU)?"LRU":(policy==MRU)?"MRU":"CLOCK";
    string new_pol = (pq.new_policy==1)?"LRU":(pq.new_policy==2)?"MRU":"CLOCK";

    if (pq.new_policy == policy) {
        tee("policy is already " + old_pol + "\n"); return;
    }

    // show pool BEFORE switch
    printPoolState("before SET POLICY " + new_pol);

    // create new manager and copy frames from old one
    ReplacementPolicy* old_mgr = bufManagers[0].second;
    policy = pq.new_policy;
    ReplacementPolicy* new_mgr = nullptr;
    if      (policy == LRU)   new_mgr = new LRUBufferManager(num_frames);
    else if (policy == MRU)   new_mgr = new MRUBufferManager(num_frames);
    else                      new_mgr = new ClockBufferManager(num_frames);

    // transfer frames — no disk reads, no flush
    new_mgr->copyFramesFrom(old_mgr);

    delete old_mgr;
    bufManagers.clear();
    if      (policy == LRU)   bufManagers.push_back({"LRU",   new_mgr});
    else if (policy == MRU)   bufManagers.push_back({"MRU",   new_mgr});
    else                      bufManagers.push_back({"CLOCK", new_mgr});

    tee("\npolicy changed: " + old_pol + " -> " + new_pol + "\n");
    tee("frames preserved — no data was evicted\n");

    // show pool AFTER switch
    printPoolState("after SET POLICY " + new_pol);
}

// ============================================================
// execCreate
// ============================================================
void QueryExecutor::execCreate(const ParsedQuery& pq) {
    if (!catalog.createTable(pq.table_name, pq.columns)) {
        tee("[Catalog] Table '" + pq.table_name + "' already exists.\n"); return;
    }
    const TableDef& tdef = catalog.getTable(pq.table_name);
    FILE* fp = fopen(tdef.filename.c_str(), "wb");
    if (!fp) { tee("[Error] Cannot create " + tdef.filename + "\n"); return; }
    char blank[PAGE_SIZE] = {};
    fwrite(blank, PAGE_SIZE, 1, fp);
    fclose(fp);
    tee("[OK] Created table '" + pq.table_name + "'\n");
}

// ============================================================
// execDrop
// ============================================================
void QueryExecutor::execDrop(const ParsedQuery& pq) {
    if (!catalog.hasTable(pq.table_name)) {
        tee("[Error] Table '" + pq.table_name + "' not found.\n"); return;
    }
    string fname = catalog.getTable(pq.table_name).filename;
    auto it = readHandles.find(pq.table_name);
    if (it != readHandles.end()) { fclose(it->second); readHandles.erase(it); }
    tableAccessCount.erase(pq.table_name);
    catalog.dropTable(pq.table_name);
    remove(fname.c_str());
    tee("[OK] Dropped table '" + pq.table_name + "'\n");
}

// ============================================================
// execAlterAdd
// ============================================================
void QueryExecutor::execAlterAdd(const ParsedQuery& pq) {
    if (!catalog.hasTable(pq.table_name)) {
        tee("[Error] Table '" + pq.table_name + "' not found.\n"); return;
    }
    if (pq.columns.empty()) { tee("[Error] No column specified.\n"); return; }
    const ColumnDef& new_col = pq.columns[0];
    TableDef old_tdef = catalog.getTable(pq.table_name);
    if (!catalog.addColumn(pq.table_name, new_col)) {
        tee("[Error] Column '" + new_col.name + "' already exists.\n"); return;
    }
    const TableDef& new_tdef = catalog.getTable(pq.table_name);
    FILE* fp = fopen(old_tdef.filename.c_str(), "rb");
    if (!fp) { tee("[Error] Cannot open " + old_tdef.filename + "\n"); return; }
    vector<vector<char>> records;
    fseek(fp, 0, SEEK_END);
    int np = (int)(ftell(fp)/PAGE_SIZE);
    for (int pg = 0; pg < np; ++pg) {
        char page[PAGE_SIZE] = {};
        fseek(fp, (long)pg*PAGE_SIZE, SEEK_SET);
        { [[maybe_unused]] size_t _r = fread(page, PAGE_SIZE, 1, fp); }
        int rc=0; memcpy(&rc, page, sizeof(int));
        int offset = sizeof(int);
        for (int r = 0; r < rc; ++r) {
            vector<char> rec(old_tdef.record_size, 0);
            memcpy(rec.data(), page+offset, old_tdef.record_size);
            records.push_back(rec);
            offset += old_tdef.record_size;
        }
    }
    fclose(fp);
    FILE* wp = fopen(new_tdef.filename.c_str(), "wb");
    if (!wp) { tee("[Error] Cannot rewrite " + new_tdef.filename + "\n"); return; }
    int mpp = (PAGE_SIZE-(int)sizeof(int))/new_tdef.record_size;
    if (records.empty()) {
        char page[PAGE_SIZE] = {}; fwrite(page, PAGE_SIZE, 1, wp);
    } else {
        size_t written = 0;
        while (written < records.size()) {
            char page[PAGE_SIZE] = {};
            int cnt = (int)min((size_t)mpp, records.size()-written);
            memcpy(page, &cnt, sizeof(int));
            int off = sizeof(int);
            for (int r = 0; r < cnt; ++r, ++written) {
                memcpy(page+off, records[written].data(), old_tdef.record_size);
                off += new_tdef.record_size;
            }
            fwrite(page, PAGE_SIZE, 1, wp);
        }
    }
    fclose(wp);
    auto it = readHandles.find(pq.table_name);
    if (it != readHandles.end()) {
        FILE* old_fp = it->second;
        fseek(old_fp, 0, SEEK_END);
        int old_np = (int)(ftell(old_fp)/PAGE_SIZE);
        for (int pg = 0; pg < old_np; ++pg)
            for (auto& bm : bufManagers) bm.second->invalidatePage(old_fp, pg);
        fclose(old_fp); readHandles.erase(it);
    }
    tee("[OK] Added column '" + new_col.name + "' to '" + pq.table_name + "'\n");
}

// ============================================================
// execInsert
// ============================================================
void QueryExecutor::execInsert(const ParsedQuery& pq) {
    if (!catalog.hasTable(pq.table_name)) {
        tee("[Error] Table '" + pq.table_name + "' not found.\n"); return;
    }
    const TableDef& tdef = catalog.getTable(pq.table_name);
    vector<string> ordered_vals(tdef.columns.size(), "");
    if (!pq.col_names.empty()) {
        if (pq.col_names.size() != pq.values.size()) {
            tee("[Error] Column count doesn't match value count.\n"); return;
        }
        for (size_t ci = 0; ci < pq.col_names.size(); ++ci) {
            bool matched = false;
            for (size_t ti = 0; ti < tdef.columns.size(); ++ti)
                if (tdef.columns[ti].name==pq.col_names[ci]) { ordered_vals[ti]=pq.values[ci]; matched=true; break; }
            if (!matched) { tee("[Error] Unknown column: " + pq.col_names[ci] + "\n"); return; }
        }
    } else {
        if (pq.values.size() != tdef.columns.size()) {
            tee("[Error] Expected " + to_string(tdef.columns.size()) + " values, got " + to_string(pq.values.size()) + "\n"); return;
        }
        ordered_vals = pq.values;
    }
    for (size_t ci = 0; ci < tdef.columns.size(); ++ci)
        if (tdef.columns[ci].type=="INT" && !ordered_vals[ci].empty())
            try { stoi(ordered_vals[ci]); }
            catch(...) { tee("[Error] Invalid integer for '" + tdef.columns[ci].name + "'\n"); return; }
    FILE* fp = fopen(tdef.filename.c_str(), "r+b");
    if (!fp) { tee("[Error] Cannot open " + tdef.filename + "\n"); return; }
    int mpp = (PAGE_SIZE-(int)sizeof(int))/tdef.record_size;
    fseek(fp, 0, SEEK_END);
    int num_pages = max(1,(int)(ftell(fp)/PAGE_SIZE));
    int last_page = num_pages-1;
    char page[PAGE_SIZE] = {};
    fseek(fp, (long)last_page*PAGE_SIZE, SEEK_SET);
    { [[maybe_unused]] size_t _r = fread(page, PAGE_SIZE, 1, fp); }
    int rec_count=0; memcpy(&rec_count, page, sizeof(int));
    if (rec_count >= mpp) { memset(page,0,PAGE_SIZE); rec_count=0; last_page=num_pages; }
    int rec_offset = sizeof(int)+rec_count*tdef.record_size;
    for (size_t ci = 0; ci < tdef.columns.size(); ++ci) {
        const ColumnDef& col = tdef.columns[ci];
        const string& val    = ordered_vals[ci];
        if (col.type=="INT") { int iv=val.empty()?0:stoi(val); memcpy(page+rec_offset+col.offset,&iv,sizeof(int)); }
        else { char buf[256]={}; int cp=min((int)val.size(),col.size-1); memcpy(buf,val.c_str(),cp); memcpy(page+rec_offset+col.offset,buf,col.size); }
    }
    rec_count++;
    memcpy(page, &rec_count, sizeof(int));
    fseek(fp, (long)last_page*PAGE_SIZE, SEEK_SET);
    fwrite(page, PAGE_SIZE, 1, fp);
    fflush(fp); fclose(fp);
    auto rh = readHandles.find(pq.table_name);
    if (rh != readHandles.end()) {
        for (auto& bm : bufManagers) bm.second->invalidatePage(rh->second, last_page);
        fclose(rh->second); readHandles.erase(rh);
    }
    tee("[OK] Inserted 1 row into '" + pq.table_name + "'\n");
}

// ============================================================
// execSelect
// ============================================================
void QueryExecutor::execSelect(const ParsedQuery& pq) {
    if (!catalog.hasTable(pq.table_name)) {
        tee("[Error] Table '" + pq.table_name + "' not found.\n"); return;
    }
    const TableDef& tdef = catalog.getTable(pq.table_name);
    FILE* fp = getReadHandle(pq.table_name);
    if (!fp) return;
    fseek(fp, 0, SEEK_END);
    int num_pages = (int)(ftell(fp)/PAGE_SIZE);

    long est_io = costSelect(num_pages);
    tee("[Info] Table '" + pq.table_name + "': " + to_string(num_pages) + " page(s) | Est I/O: " + to_string(est_io) + "\n");

    printPoolState("before SELECT " + pq.table_name);

    bool repeated = (tableAccessCount[pq.table_name] > 0);
    tableAccessCount[pq.table_name]++;
    printHint(repeated ? "SELECT_REPEAT" : "SELECT", pq.table_name);

    vector<string> headers;
    bool has_agg  = false;
    bool star_mode = (!pq.select_cols.empty() && pq.select_cols[0].star);
    if (star_mode) {
        for (auto& c : tdef.columns) headers.push_back(c.name);
    } else {
        for (auto& sc : pq.select_cols) {
            if (sc.star) { for (auto& c : tdef.columns) headers.push_back(c.name); }
            else if (sc.agg != AggFunc::NONE) {
                has_agg = true;
                string lbl;
                switch(sc.agg) {
                    case AggFunc::COUNT: lbl="count"; break; case AggFunc::SUM: lbl="sum"; break;
                    case AggFunc::AVG:   lbl="avg";   break; case AggFunc::MIN: lbl="min"; break;
                    case AggFunc::MAX:   lbl="max";   break; default: break;
                }
                lbl += "(" + sc.col + ")";
                headers.push_back(sc.alias.empty() ? lbl : sc.alias);
            } else {
                headers.push_back(sc.alias.empty() ? sc.col : sc.alias);
            }
        }
    }

    struct RawRow { vector<string> vals; };
    vector<RawRow> raw_rows;
    for (int pg = 0; pg < num_pages; ++pg) {
        char* page = nullptr;
        for (auto& bm : bufManagers) { char* p=bm.second->getPage(fp,pg); if(!page) page=p; }
        if (!page) continue;
        int rc=0; memcpy(&rc, page, sizeof(int));
        int off = sizeof(int);
        for (int r = 0; r < rc; ++r) {
            if (matchesWhere(page, off, tdef, pq.where)) {
                RawRow rr;
                for (auto& col : tdef.columns) rr.vals.push_back(readField(page,off,col));
                raw_rows.push_back(rr);
            }
            off += tdef.record_size;
        }
        for (auto& bm : bufManagers) bm.second->unpinPage(fp, pg);
    }

    vector<Row> result_rows;
    if (!pq.group_by.empty() || has_agg) {
        vector<int> gb_idx;
        for (auto& gname : pq.group_by)
            for (int ci=0; ci<(int)tdef.columns.size(); ++ci)
                if (tdef.columns[ci].name==gname) { gb_idx.push_back(ci); break; }
        map<vector<string>, vector<RawRow>> groups;
        for (auto& rr : raw_rows) {
            vector<string> key;
            for (int idx : gb_idx) key.push_back(rr.vals[idx]);
            groups[key].push_back(rr);
        }
        if (gb_idx.empty() && has_agg) groups[{}] = raw_rows;
        for (auto& [key, grp] : groups) {
            Row row;
            if (star_mode) { if (!grp.empty()) for (auto& v : grp[0].vals) row.cols.push_back(v); }
            else {
                for (auto& sc : pq.select_cols) {
                    if (sc.star) { if (!grp.empty()) for (auto& v : grp[0].vals) row.cols.push_back(v); continue; }
                    if (sc.agg==AggFunc::NONE) {
                        string val;
                        for (int ci=0; ci<(int)tdef.columns.size(); ++ci)
                            if (tdef.columns[ci].name==sc.col) { val=grp.empty()?"":grp[0].vals[ci]; break; }
                        row.cols.push_back(val); continue;
                    }
                    int col_idx=-1; string col_type="INT";
                    for (int ci=0; ci<(int)tdef.columns.size(); ++ci)
                        if (tdef.columns[ci].name==sc.col||sc.col=="*") { col_idx=ci; col_type=tdef.columns[ci].type; break; }
                    if (sc.agg==AggFunc::COUNT) { row.cols.push_back(to_string((int)grp.size())); continue; }
                    double sum=0,mn=1e18,mx=-1e18; int cnt=0;
                    for (auto& rr : grp) {
                        if (col_idx<0) continue;
                        try { double v=stod(rr.vals[col_idx]); sum+=v; cnt++; mn=min(mn,v); mx=max(mx,v); } catch(...) {}
                    }
                    ostringstream oss; oss<<fixed<<setprecision(2);
                    if      (sc.agg==AggFunc::SUM) oss<<sum;
                    else if (sc.agg==AggFunc::AVG) oss<<(cnt?sum/cnt:0.0);
                    else if (sc.agg==AggFunc::MIN) oss<<(cnt?mn:0.0);
                    else if (sc.agg==AggFunc::MAX) oss<<(cnt?mx:0.0);
                    row.cols.push_back(oss.str());
                }
            }
            result_rows.push_back(row);
        }
        if (!pq.having.isEmpty()) {
            vector<Row> filtered;
            for (auto& row : result_rows) {
                bool ok=true;
                for (size_t ci=0; ci<pq.having.conditions.size(); ++ci) {
                    const Condition& cond=pq.having.conditions[ci];
                    string fval, needle=cond.col1;
                    for (size_t hi=0; hi<headers.size(); ++hi) {
                        if (hi>=row.cols.size()) break;
                        const string& hdr=headers[hi];
                        bool hit=(hdr==needle)||(hdr.size()>needle.size()&&hdr.substr(0,needle.size())==needle&&hdr[needle.size()]=='(');
                        if (hit) { fval=row.cols[hi]; break; }
                    }
                    bool cval=evalCondition(fval,cond.op,cond.value,"INT");
                    if (ci==0) ok=cval;
                    else if (pq.having.connectors[ci-1]=="AND") ok=ok&&cval;
                    else                                         ok=ok||cval;
                }
                if (ok) filtered.push_back(row);
            }
            result_rows=filtered;
        }
    } else {
        for (auto& rr : raw_rows) {
            Row row;
            if (star_mode) { row.cols=rr.vals; }
            else {
                for (auto& sc : pq.select_cols) {
                    if (sc.star) { row.cols.insert(row.cols.end(),rr.vals.begin(),rr.vals.end()); continue; }
                    if (sc.agg!=AggFunc::NONE) { row.cols.push_back(""); continue; }
                    bool found=false;
                    for (int ci=0; ci<(int)tdef.columns.size(); ++ci)
                        if (tdef.columns[ci].name==sc.col) { row.cols.push_back(rr.vals[ci]); found=true; break; }
                    if (!found) row.cols.push_back("NULL");
                }
            }
            result_rows.push_back(row);
        }
    }

    if (pq.distinct) {
        set<vector<string>> seen; vector<Row> deduped;
        for (auto& r : result_rows) if (seen.insert(r.cols).second) deduped.push_back(r);
        result_rows=deduped;
    }

    if (!pq.order_by.empty()) {
        vector<int> ord_idx; vector<bool> ord_asc;
        for (size_t oi=0; oi<pq.order_by.size(); ++oi) {
            int idx=-1;
            for (int hi=0; hi<(int)headers.size(); ++hi)
                if (headers[hi]==pq.order_by[oi]) { idx=hi; break; }
            if (idx==-1)
                for (int ci=0; ci<(int)tdef.columns.size(); ++ci)
                    if (tdef.columns[ci].name==pq.order_by[oi]) { idx=ci; break; }
            ord_idx.push_back(idx);
            ord_asc.push_back(oi<pq.order_asc.size()?pq.order_asc[oi]:true);
        }
        sort(result_rows.begin(),result_rows.end(),[&](const Row& a,const Row& b){
            for (size_t k=0; k<ord_idx.size(); ++k) {
                int idx=ord_idx[k]; if(idx<0) continue;
                string va=idx<(int)a.cols.size()?a.cols[idx]:"";
                string vb=idx<(int)b.cols.size()?b.cols[idx]:"";
                bool num=true; long ia=0,ib=0;
                try{ia=stol(va);ib=stol(vb);}catch(...){num=false;}
                int cmp=num?(ia<ib?-1:ia>ib?1:0):va.compare(vb);
                if(cmp!=0) return ord_asc[k]?cmp<0:cmp>0;
            }
            return false;
        });
    }

    int start=min(pq.offset,(int)result_rows.size());
    int end=pq.limit>=0?min(start+pq.limit,(int)result_rows.size()):(int)result_rows.size();
    vector<Row> paged(result_rows.begin()+start,result_rows.begin()+end);

    vector<int> widths=computeWidths(headers,paged);
    tee("\nSELECT FROM " + pq.table_name + "\n");
    printHeader(headers,widths);
    for (auto& r : paged) printRow(r,widths);
    printSep(widths);
    string rowline = to_string(paged.size()) + " row(s) returned";
    if (pq.offset>0||pq.limit>=0)
        rowline += "  (total before limit: " + to_string(result_rows.size()) + ")";
    tee(rowline + "\n");

    writeStats("SELECT " + pq.table_name);

    BufStats s = bufManagers[0].second->getStats();
    tee("\ncost summary\n");
    tee("  estimated I/O  : " + to_string(est_io) + " page reads\n");
    tee("  actual reads   : " + to_string(s.diskreads) + "\n");

    printPoolState("after SELECT " + pq.table_name);
}

// ============================================================
// execJoin — NLJ vs BNLJ cost comparison, execute cheaper one
// ============================================================
void QueryExecutor::execJoin(const ParsedQuery& pq) {
    if (!catalog.hasTable(pq.table1)||!catalog.hasTable(pq.table2)) {
        tee("[Error] One or both tables not found.\n"); return;
    }
    const TableDef& td1=catalog.getTable(pq.table1);
    const TableDef& td2=catalog.getTable(pq.table2);
    FILE* fp1=getReadHandle(pq.table1);
    FILE* fp2=getReadHandle(pq.table2);
    if (!fp1||!fp2) return;

    int np1=getNumPages(fp1), np2=getNumPages(fp2);

    // Always put smaller table as outer (better for both algorithms)
    string outer_name=pq.table1, inner_name=pq.table2;
    FILE *fp_o=fp1, *fp_i=fp2;
    const TableDef *td_o=&td1, *td_i=&td2;
    int np_o=np1, np_i=np2;
    if (np2 < np1) {
        swap(outer_name,inner_name); swap(fp_o,fp_i);
        swap(td_o,td_i); swap(np_o,np_i);
    }

    // Resolve join columns
    string jn1=pq.on_cond.col1, jn2=pq.on_cond.col2.empty()?pq.on_cond.value:pq.on_cond.col2;
    int jcol_o=-1, jcol_i=-1;
    for (int c=0; c<(int)td_o->columns.size(); ++c)
        if (td_o->columns[c].name==jn1||td_o->columns[c].name==jn2) jcol_o=c;
    for (int c=0; c<(int)td_i->columns.size(); ++c)
        if (td_i->columns[c].name==jn1||td_i->columns[c].name==jn2) jcol_i=c;
    if (jcol_o<0||jcol_i<0) {
        tee("[Error] Join column not found: '" + jn1 + "' or '" + jn2 + "'\n"); return;
    }

    // ── Cost comparison ──────────────────────────────────────
    // NLJ  formula: M_R + M_S * M_R          (needs only B=3)
    // BNLJ formula: M_R + M_S * ceil(M_R/(B-2))
    long est_nlj  = costNLJ (np_o, np_i);
    long est_bnlj = costBNLJ(np_o, np_i, num_frames);
    bool use_bnlj = (est_bnlj <= est_nlj);
    int  block_sz = use_bnlj ? max(1, num_frames-2) : 1;  // NLJ uses block_sz=1
    string chosen = use_bnlj ? "BNLJ" : "NLJ";

    // show pool state before the join runs
    printPoolState("before JOIN " + outer_name + " x " + inner_name);
    printBufferHint(np_o, np_i);
    tee("\njoin cost comparison\n");
    tee("  outer table  : " + outer_name + "  (" + to_string(np_o) + " pages)\n");
    tee("  inner table  : " + inner_name + "  (" + to_string(np_i) + " pages)\n");
    tee("  buffer frames: " + to_string(num_frames) + "\n");
    tee("\n");
    tee("  algorithm    formula                        est I/O\n");
    tee("  ---------    -------                        -------\n");
    {
        ostringstream f1, f2;
        f1 << np_o << " + " << np_i << " x " << np_o;
        f2 << np_o << " + " << np_i << " x ceil(" << np_o << "/" << max(1,num_frames-2) << ")";
        tee("  NLJ          " + f1.str() + string(max(1,(int)(31-f1.str().size())),' ') + to_string(est_nlj) + "\n");
        tee("  BNLJ         " + f2.str() + string(max(1,(int)(31-f2.str().size())),' ') + to_string(est_bnlj) + "\n");
    }
    tee("\n");
    tee("  chosen: " + chosen);
    if (use_bnlj) tee("  (BNLJ is cheaper or equal with B=" + to_string(num_frames) + ")\n");
    else          tee("  (NLJ is cheaper — buffer too small to help)\n");
    tee("\n");

    tableAccessCount[pq.table1]++;
    tableAccessCount[pq.table2]++;

    // Build result headers
    vector<string> headers;
    for (auto& c : td_o->columns) headers.push_back(outer_name+"."+c.name);
    for (auto& c : td_i->columns) headers.push_back(inner_name+"."+c.name);

    // ── Execution (NLJ = BNLJ with block_sz=1, both go through buffer mgr) ──
    vector<Row> result_rows;
    string jtype = td_o->columns[jcol_o].type;

    for (int outer_start=0; outer_start<np_o; outer_start+=block_sz) {
        int outer_end = min(outer_start+block_sz, np_o);

        // Load outer block (1 page for NLJ, B-2 pages for BNLJ)
        vector<pair<char*,int>> outer_pages;
        for (int i=outer_start; i<outer_end; ++i) {
            char* page=nullptr;
            for (auto& bm : bufManagers) { char* p=bm.second->getPage(fp_o,i); if(!page) page=p; }
            if (!page) break;
            outer_pages.push_back({page,i});
        }

        // Scan entire inner table once per outer block
        for (int j=0; j<np_i; ++j) {
            char* inner=nullptr;
            for (auto& bm : bufManagers) { char* p=bm.second->getPage(fp_i,j); if(!inner) inner=p; }
            if (!inner) break;
            int rc2=0; memcpy(&rc2,inner,sizeof(int));

            for (auto& [op,_pn] : outer_pages) {
                int rc1=0; memcpy(&rc1,op,sizeof(int));
                int off1=sizeof(int);
                for (int r1=0; r1<rc1; ++r1) {
                    bool t1_ok=pq.join_where.isEmpty()||matchesWhere(op,off1,*td_o,pq.join_where);
                    if (t1_ok) {
                        string jval1=readField(op,off1,td_o->columns[jcol_o]);
                        int off2=sizeof(int);
                        for (int r2=0; r2<rc2; ++r2) {
                            string jval2=readField(inner,off2,td_i->columns[jcol_i]);
                            if (evalCondition(jval1,pq.on_cond.op,jval2,jtype)) {
                                Row row;
                                for (auto& col : td_o->columns) row.cols.push_back(readField(op,off1,col));
                                for (auto& col : td_i->columns) row.cols.push_back(readField(inner,off2,col));
                                result_rows.push_back(row);
                            }
                            off2+=td_i->record_size;
                        }
                    }
                    off1+=td_o->record_size;
                }
            }
            for (auto& bm : bufManagers) bm.second->unpinPage(fp_i,j);
        }
        for (auto& [_pg,pn] : outer_pages)
            for (auto& bm : bufManagers) bm.second->unpinPage(fp_o,pn);
    }

    // ORDER BY
    if (!pq.order_by.empty()) {
        vector<int> ord_idx; vector<bool> ord_asc;
        for (size_t oi=0; oi<pq.order_by.size(); ++oi) {
            int idx=-1;
            for (int hi=0; hi<(int)headers.size(); ++hi)
                if (headers[hi]==pq.order_by[oi]) { idx=hi; break; }
            ord_idx.push_back(idx);
            ord_asc.push_back(oi<pq.order_asc.size()?pq.order_asc[oi]:true);
        }
        sort(result_rows.begin(),result_rows.end(),[&](const Row& a,const Row& b){
            for (size_t k=0; k<ord_idx.size(); ++k) {
                int idx=ord_idx[k]; if(idx<0) continue;
                string va=idx<(int)a.cols.size()?a.cols[idx]:"";
                string vb=idx<(int)b.cols.size()?b.cols[idx]:"";
                long ia=0,ib_=0; bool num=true;
                try{ia=stol(va);ib_=stol(vb);}catch(...){num=false;}
                int cmp=num?(ia<ib_?-1:ia>ib_?1:0):va.compare(vb);
                if(cmp!=0) return ord_asc[k]?cmp<0:cmp>0;
            }
            return false;
        });
    }

    int start=min(pq.offset,(int)result_rows.size());
    int end=pq.limit>=0?min(start+pq.limit,(int)result_rows.size()):(int)result_rows.size();
    vector<Row> paged(result_rows.begin()+start,result_rows.begin()+end);

    // Print result table
    vector<int> widths=computeWidths(headers,paged);
    tee("\n" + outer_name + " " + chosen + " " + inner_name + "\n");
    printHeader(headers,widths);
    for (auto& r : paged) printRow(r,widths);
    printSep(widths);
    tee(to_string(paged.size()) + " row(s) returned\n");

    writeStats(chosen + " " + outer_name + " x " + inner_name);

    BufStats s = bufManagers[0].second->getStats();
    tee("\ncost summary\n");
    tee("  algorithm used : " + chosen + "\n");
    tee("  NLJ  estimated : " + to_string(est_nlj)  + " I/Os\n");
    tee("  BNLJ estimated : " + to_string(est_bnlj) + " I/Os  (B=" + to_string(num_frames) + ")\n");
    tee("  actual disk reads: " + to_string(s.diskreads) + "\n");

    printPoolState("after JOIN " + outer_name + " x " + inner_name);
}

// ============================================================
// execUpdate
// ============================================================
void QueryExecutor::execUpdate(const ParsedQuery& pq) {
    if (!catalog.hasTable(pq.table_name)) {
        tee("[Error] Table '" + pq.table_name + "' not found.\n"); return;
    }
    const TableDef& tdef=catalog.getTable(pq.table_name);
    for (auto& a : pq.assignments) {
        bool found=false;
        for (auto& col : tdef.columns) {
            if (col.name==a.col) {
                found=true;
                if (col.type=="INT") { try{stoi(a.value);}catch(...){tee("[Error] Invalid int for '" + a.col + "'\n");return;} }
                break;
            }
        }
        if (!found) { tee("[Error] Unknown column '" + a.col + "'\n"); return; }
    }
    FILE* rh=getReadHandle(pq.table_name); if(!rh) return;
    fseek(rh,0,SEEK_END); int np=(int)(ftell(rh)/PAGE_SIZE);
    for (int pg=0;pg<np;++pg) for (auto& bm:bufManagers) bm.second->invalidatePage(rh,pg);
    fclose(rh); readHandles.erase(pq.table_name);
    FILE* fp=fopen(tdef.filename.c_str(),"r+b");
    if (!fp) { tee("[Error] Cannot open " + tdef.filename + "\n"); return; }
    int updated=0;
    for (int pg=0;pg<np;++pg) {
        char page[PAGE_SIZE]={}; fseek(fp,(long)pg*PAGE_SIZE,SEEK_SET);
        { [[maybe_unused]] size_t _r=fread(page,PAGE_SIZE,1,fp); }
        int rc=0;memcpy(&rc,page,sizeof(int));int off=sizeof(int);bool dirty=false;
        for (int r=0;r<rc;++r) {
            if (matchesWhere(page,off,tdef,pq.update_where)) {
                for (auto& a:pq.assignments) for (auto& col:tdef.columns) {
                    if (col.name!=a.col) continue;
                    if (col.type=="INT"){int iv=stoi(a.value);memcpy(page+off+col.offset,&iv,sizeof(int));}
                    else{char buf[256]={};int cp=min((int)a.value.size(),col.size-1);memcpy(buf,a.value.c_str(),cp);memcpy(page+off+col.offset,buf,col.size);}
                }
                dirty=true; updated++;
            }
            off+=tdef.record_size;
        }
        if (dirty){fseek(fp,(long)pg*PAGE_SIZE,SEEK_SET);fwrite(page,PAGE_SIZE,1,fp);}
    }
    fflush(fp);fclose(fp);
    tee("[OK] " + to_string(updated) + " row(s) updated in '" + pq.table_name + "'\n");
}

// ============================================================
// execDelete
// ============================================================
void QueryExecutor::execDelete(const ParsedQuery& pq) {
    if (!catalog.hasTable(pq.table_name)) {
        tee("[Error] Table '" + pq.table_name + "' not found.\n"); return;
    }
    const TableDef& tdef=catalog.getTable(pq.table_name);
    FILE* rh=getReadHandle(pq.table_name); if(!rh) return;
    fseek(rh,0,SEEK_END); int np=(int)(ftell(rh)/PAGE_SIZE);
    for (int pg=0;pg<np;++pg) for (auto& bm:bufManagers) bm.second->invalidatePage(rh,pg);
    fclose(rh); readHandles.erase(pq.table_name);
    FILE* fp=fopen(tdef.filename.c_str(),"r+b");
    if (!fp) { tee("[Error] Cannot open " + tdef.filename + "\n"); return; }
    int deleted=0;
    for (int pg=0;pg<np;++pg) {
        char pi[PAGE_SIZE]={},po[PAGE_SIZE]={};
        fseek(fp,(long)pg*PAGE_SIZE,SEEK_SET);
        { [[maybe_unused]] size_t _r=fread(pi,PAGE_SIZE,1,fp); }
        int rc=0;memcpy(&rc,pi,sizeof(int));int oi=sizeof(int),oo=sizeof(int),nc=0;
        for (int r=0;r<rc;++r) {
            if (!matchesWhere(pi,oi,tdef,pq.delete_where)) {
                memcpy(po+oo,pi+oi,tdef.record_size);oo+=tdef.record_size;nc++;
            } else { deleted++; }
            oi+=tdef.record_size;
        }
        memcpy(po,&nc,sizeof(int));
        fseek(fp,(long)pg*PAGE_SIZE,SEEK_SET);fwrite(po,PAGE_SIZE,1,fp);
    }
    fflush(fp);fclose(fp);
    tee("[OK] " + to_string(deleted) + " row(s) deleted from '" + pq.table_name + "'\n");
}