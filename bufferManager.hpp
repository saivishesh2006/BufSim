#ifndef __BUF_MANAGER_H_
#define __BUF_MANAGER_H_

#include <iostream>
#include <vector>
#include <cstring>
#include <unordered_map>
#include <list>

#define PAGE_SIZE 4096

// Defines a custom hash function for pairs of (FILE*, int)
struct PairHash {
    size_t operator()(const std::pair<FILE *, int>& p) const {
        size_t temp1 = std::hash<FILE *>()(p.first);
        size_t temp2 = std::hash<int>()(p.second);
        return temp1 ^ temp2;
    }
};

class BufStats {
public:
    int accesses;
    int diskreads;
    int pageHits;

    BufStats();
    void clear();
};

class Frame {
public:
    int page_Num;
    char* page_Data;
    FILE *fp;
    int pin_count;         // Changed to pin_count for active query operators
    bool second_chance;    // For CLOCK
    bool is_dirty;         // Dirty bit for write-backs

    Frame();
    ~Frame();

    // Delete copy constructor and assignment operator to prevent double-free segfaults
    Frame(const Frame&) = delete;
    Frame& operator=(const Frame&) = delete;

    void setFrame(FILE* fp, int page_Num, bool is_pinned);
    void unpinFrame();
    void pinFrame();
};

class ReplacementPolicy {
public:
    virtual ~ReplacementPolicy() {}
    virtual char* getPage(FILE* fp, int page_Num) = 0;
    virtual void unpinPage(FILE* fp, int page_Num) = 0;
    virtual BufStats getStats() = 0;
    virtual void clearStats() = 0;
};

class LRUBufferManager : public ReplacementPolicy {
private:
    int num_Frames;
    std::list<Frame*> lru; 
    std::unordered_map<std::pair<FILE*, int>, std::list<Frame*>::iterator, PairHash> mp;
    BufStats stats;

public:
    LRUBufferManager(int num_Frames);
    ~LRUBufferManager();
    char* getPage(FILE* fp, int page_Num) override;
    void unpinPage(FILE* fp, int page_Num) override;
    BufStats getStats() override;
    void clearStats() override;
};

class ClockBufferManager : public ReplacementPolicy {
private:
    int num_Frames;
    Frame* bufferPool; 
    int clock_hand;
    int num_Pages;
    BufStats stats;

public:
    ClockBufferManager(int num_Frames);
    ~ClockBufferManager();
    char* getPage(FILE* fp, int page_Num) override;
    void unpinPage(FILE* fp, int page_Num) override;
    BufStats getStats() override;
    void clearStats() override;
};

class MRUBufferManager : public ReplacementPolicy {
private:
    int num_Frames;
    std::list<Frame*> mru;
    std::unordered_map<std::pair<FILE*, int>, std::list<Frame*>::iterator, PairHash> mp;
    BufStats stats;

public:
    MRUBufferManager(int num_Frames);
    ~MRUBufferManager();
    char* getPage(FILE* fp, int page_Num) override;
    void unpinPage(FILE* fp, int page_Num) override;
    BufStats getStats() override;
    void clearStats() override;
};

#endif