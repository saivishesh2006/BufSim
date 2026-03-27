#ifndef __BUF_MANAGER_H_
#define __BUF_MANAGER_H_

#include <iostream>
#include <vector>
#include <string.h>
#include <string>
#include <unordered_map>
#include <iterator>
#include <list>
using namespace std;

#define PAGE_SIZE 4096

// // implementing buffer manager for LRU using Pinned and Unpinned pages 
// class of Frame which is an entity in Memory to store a Page

class Frame{

    private:
    int page_Num;       // page number of page
    char* page_Data;    // data in page
    FILE *fp;          // file to which this page belongs to
    bool pinned;       // either pinned or unpinned
    bool second_chance;         // for clock replacement algorithm

    void setFrame(FILE*fp, int page_Num, char* page_Data, bool pinned);
    void unpinFrame();

    public:
    Frame();
    Frame(const Frame& f);
    ~Frame();
    friend class LRUBufferManager;
    friend class ClockBufferManager;
    friend class MRUBufferManager;
    
};

//Defines a custom hash function for pairs of (FILE*, int) used as keys in maps.
struct PairHash {
    size_t operator()(const pair<FILE *, int>& p) const {
        size_t temp1 = hash<FILE *>()(p.first);
        size_t temp2 = hash<int>()(p.second);
        return temp1 ^ temp2;
    }
};

//Keeps track of buffer manager statistics such as accesses, disk reads, and page hits.
class BufStats{
    public:
    int accesses;
    int diskreads;
    int pageHits;

    BufStats();
    void clear();
};

class ReplacementPolicy {
public:
    virtual ~ReplacementPolicy() {}
    virtual char* getPage(FILE*fp, int page_Num) = 0;
    virtual void unpinPage(FILE*fp, int page_Num) = 0;
    virtual BufStats getStats() = 0;
    virtual void clearStats() = 0;
};

class LRUBufferManager: public ReplacementPolicy{

    private:
    int num_Frames;    // number of frames that can be fit in pool
    list<Frame> lru;  // list to implement LRU
    unordered_map<pair<FILE*, int>, list<Frame>::iterator, PairHash> mp;   // map to identify whether a page is present in buffer or not
    BufStats stats;

    public:
    LRUBufferManager(int num_Frames);
    char* getPage(FILE*fp, int page_Num);
    ~LRUBufferManager();
    BufStats getStats();
    void clearStats();
    void unpinPage(FILE*fp, int page_Num);
};

// implement clock replacementr algorithm
class ClockBufferManager: public ReplacementPolicy{

    private:
    int num_Frames;    // number of frames that can be fit in pool
    Frame* bufferPool;  // list to implement clock
    int clock_hand;   // clock hand
    BufStats stats;
    int num_Pages;

    public:
    ClockBufferManager(int num_Frames);
    char* getPage(FILE*fp, int page_Num);
    ~ClockBufferManager();
    void unpinPage(FILE*fp, int page_Num);
    BufStats getStats();
    void clearStats();
};

class MRUBufferManager: public ReplacementPolicy{

    private:
    int num_Frames;    // number of frames that can be fit in pool
    list<Frame> mru;  // list to implement MRU
    unordered_map<pair<FILE*, int>, list<Frame>::iterator, PairHash> mp;   // map to identify whether a page is present in buffer or not
    BufStats stats;

    public:
    MRUBufferManager(int num_Frames);
    char* getPage(FILE*fp, int page_Num);
    ~MRUBufferManager();
    BufStats getStats();
    void clearStats();
    void unpinPage(FILE*fp, int page_Num);
};
#endif