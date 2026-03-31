#include "queryProcessor.hpp"
#include <iostream>
#include <cstring>
#include <cstdlib>

using namespace std;

QueryProcessor::QueryProcessor(int num_Frames, int replacementPolicy) {
    if (replacementPolicy == CLOCK) bufferManager = new ClockBufferManager(num_Frames);
    else if (replacementPolicy == LRU) bufferManager = new LRUBufferManager(num_Frames);
    else if (replacementPolicy == MRU) bufferManager = new MRUBufferManager(num_Frames);
    else {
        cerr << "Invalid replacement policy\n";
        exit(1);
    }
}

QueryProcessor::~QueryProcessor() {
    delete bufferManager;
}

void QueryProcessor::processSelectQuery(FILE *fp, int col1, string value) {
    int numPages = 0;
    int rSize = 20 * sizeof(char) + 2 * sizeof(int);

    fseek(fp, 0, SEEK_END);
    numPages = ftell(fp) / PAGE_SIZE;
    fseek(fp, 0, SEEK_SET);

    // Optimization: Parse target integer ONCE outside the loop
    int target_int = (col1 == 2 || col1 == 3) ? atoi(value.c_str()) : 0;

    for (int i = 0; i < numPages; ++i) {
        char *page_Data = bufferManager->getPage(fp, i);
        if (!page_Data) continue;

        int numRecords;
        memcpy(&numRecords, page_Data, sizeof(int));
        
        // Use offset instead of modifying the pointer
        int offset = sizeof(int); 

        while (numRecords--) {
            char name[20];
            int age, weight;
            
            memcpy(name, page_Data + offset, 20);
            memcpy(&age, page_Data + offset + 20, sizeof(int));
            memcpy(&weight, page_Data + offset + 20 + sizeof(int), sizeof(int));
            offset += rSize;

            bool match = false;
            if (col1 == 1 && value == name) match = true;
            else if (col1 == 2 && age == target_int) match = true;
            else if (col1 == 3 && weight == target_int) match = true;

            if (match) {
                // cout << name << " " << age << " " << weight << "\n";
            }
        }
        bufferManager->unpinPage(fp, i);
    }
    
    cout << "\n--- Select Query Stats ---\n";
    cout << "Page Accesses: " << bufferManager->getStats().accesses << "\n";
    cout << "Disk Reads: " << bufferManager->getStats().diskreads << "\n";
    cout << "Page Hits: " << bufferManager->getStats().pageHits << "\n";
}

BufStats QueryProcessor::processJoinQuery(FILE *fp1, FILE *fp2, int col1, int col2) {
    int numPages1 = 0, numPages2 = 0;
    int rSize = 20 * sizeof(char) + 2 * sizeof(int);

    fseek(fp1, 0, SEEK_END);
    numPages1 = ftell(fp1) / PAGE_SIZE;
    
    fseek(fp2, 0, SEEK_END);
    numPages2 = ftell(fp2) / PAGE_SIZE;

    for (int i = 0; i < numPages1; ++i) {
        char* page_Data1 = bufferManager->getPage(fp1, i);        
        if (page_Data1 == nullptr) {
            cerr << "Buffer stalled on outer loop!\n";
            exit(1);
        }

        for (int j = 0; j < numPages2; ++j) {
            char* page_Data2 = bufferManager->getPage(fp2, j);
            if (page_Data2 == nullptr) {
                cerr << "Buffer stalled on inner loop!\n";
                exit(1);
            }

            int page1Offset = 0;
            int numRecords1;
            memcpy(&numRecords1, page_Data1, sizeof(int));
            page1Offset += sizeof(int);

            while (numRecords1--) {
                char name1[20];
                int age1, weight1;
                memcpy(name1, page_Data1 + page1Offset, 20);
                memcpy(&age1, page_Data1 + page1Offset + 20, sizeof(int));
                memcpy(&weight1, page_Data1 + page1Offset + 20 + sizeof(int), sizeof(int));
                
                int page2Offset = 0;
                int numRecords2;
                memcpy(&numRecords2, page_Data2, sizeof(int));
                page2Offset += sizeof(int);

                while (numRecords2--) {
                    char name2[20];
                    int age2, weight2;
                    memcpy(name2, page_Data2 + page2Offset, 20);
                    memcpy(&age2, page_Data2 + page2Offset + 20, sizeof(int));
                    memcpy(&weight2, page_Data2 + page2Offset + 20 + sizeof(int), sizeof(int));
                    page2Offset += rSize;

                    // Optimized Comparison: Native types instead of strings
                    bool match = false;
                    if (col1 == 1 && col2 == 1 && strcmp(name1, name2) == 0) match = true;
                    else if (col1 == 2 && col2 == 2 && age1 == age2) match = true;
                    else if (col1 == 3 && col2 == 3 && weight1 == weight2) match = true;
                    // ... (Add other cross-type comparisons if needed, though rare in normal joins)

                    if (match) {
                        // cout << name1 << " " << age1 << " " << weight1 << " | " << name2 << " " << age2 << " " << weight2 << "\n";
                    }
                }
                page1Offset += rSize;
            }
            bufferManager->unpinPage(fp2, j);
        }
        bufferManager->unpinPage(fp1, i);
    }

    // cout << "\n--- Join Query Stats ---\n";
    // cout << "Page Accesses: " << bufferManager->getStats().accesses << "\n";
    // cout << "Disk Reads: " << bufferManager->getStats().diskreads << "\n";
    // cout << "Page Hits: " << bufferManager->getStats().pageHits << "\n";
    return bufferManager->getStats();
}