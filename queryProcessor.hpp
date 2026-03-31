#ifndef __QUERY_PROCESSOR_H_
#define __QUERY_PROCESSOR_H_

#include "bufferManager.hpp"
#include <string>

#define LRU 1
#define MRU 2
#define CLOCK 3

class QueryProcessor {
private:
    ReplacementPolicy* bufferManager;
    
public:
    QueryProcessor(int num_Frames, int replacementPolicy);
    ~QueryProcessor(); // Added destructor to clean up memory
    
    void processSelectQuery(FILE *fp, int col1, std::string value);
    BufStats processJoinQuery(FILE *fp1, FILE *fp2, int col1, int col2);
};

#endif