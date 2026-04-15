#ifndef __BUF_MANAGER_H_
#define __BUF_MANAGER_H_

#include <iostream>
#include <vector>
#include <cstring>
#include <unordered_map>
#include <list>

#define PAGE_SIZE 4096

// Better hash for (FILE*, int) pairs: avoids plain XOR collisions
struct PairHash {
    size_t operator()(const std::pair<FILE*, int>& p) const {
        size_t h1 = std::hash<FILE*>()(p.first);
        size_t h2 = std::hash<int>()(p.second);
        return h1 ^ (h2 * 2654435761u);
    }
};

// -------------------------------------------------------
// BufStats
// -------------------------------------------------------
class BufStats {
public:
    int accesses;
    int diskreads;
    int pageHits;
    BufStats();
    void clear();
};

// -------------------------------------------------------
// Frame: one slot in the buffer pool
// -------------------------------------------------------
class Frame {
public:
    int   page_Num;
    char* page_Data;
    FILE* fp;
    int   pin_count;
    bool  second_chance;
    bool  is_dirty;

    Frame();
    ~Frame();
    Frame(const Frame&)            = delete;
    Frame& operator=(const Frame&) = delete;

    void setFrame(FILE* fp, int page_Num, bool is_pinned);
    void unpinFrame();
    void pinFrame();
};

// -------------------------------------------------------
// FrameInfo — snapshot of one buffer frame for display
// -------------------------------------------------------
struct FrameInfo {
    FILE* fp       = nullptr;
    int   page_num = -1;
    int   pin_count = 0;
    bool  second_chance = false;  // CLOCK only
    int   recency_rank  = -1;     // 1=most recent, for LRU/MRU order
    bool  occupied = false;
};

// -------------------------------------------------------
// Abstract interface
// -------------------------------------------------------
class ReplacementPolicy {
public:
    virtual ~ReplacementPolicy() {}
    virtual char* getPage       (FILE* fp, int page_Num) = 0;
    virtual void  unpinPage     (FILE* fp, int page_Num) = 0;
    virtual void  invalidatePage(FILE* fp, int page_Num) = 0;
    virtual BufStats getStats()   = 0;
    virtual void     clearStats() = 0;
    // Return snapshot of all frames for display
    virtual std::vector<FrameInfo> getSnapshot() = 0;
    // Copy frames from another manager (for SET POLICY transfer)
    virtual void copyFramesFrom(ReplacementPolicy* other) = 0;
};

// -------------------------------------------------------
// LRU
// -------------------------------------------------------
class LRUBufferManager : public ReplacementPolicy {
private:
    int num_Frames;
    std::list<Frame*> lru;
    std::unordered_map<std::pair<FILE*,int>,
                       std::list<Frame*>::iterator, PairHash> mp;
    BufStats stats;
public:
    explicit LRUBufferManager(int num_Frames);
    ~LRUBufferManager() override;
    char* getPage       (FILE* fp, int page_Num) override;
    void  unpinPage     (FILE* fp, int page_Num) override;
    void  invalidatePage(FILE* fp, int page_Num) override;
    BufStats getStats()   override;
    void     clearStats() override;
    std::vector<FrameInfo> getSnapshot()              override;
    void copyFramesFrom(ReplacementPolicy* other)     override;
};

// -------------------------------------------------------
// CLOCK — FIX: unordered_map for O(1) lookup;
//              all-pinned escape so loop can't spin forever
// -------------------------------------------------------
class ClockBufferManager : public ReplacementPolicy {
private:
    int    num_Frames;
    Frame* bufferPool;
    int    clock_hand;
    int    num_Pages;
    std::unordered_map<std::pair<FILE*,int>, int, PairHash> mp; // -> frame index
    BufStats stats;
public:
    explicit ClockBufferManager(int num_Frames);
    ~ClockBufferManager() override;
    char* getPage       (FILE* fp, int page_Num) override;
    void  unpinPage     (FILE* fp, int page_Num) override;
    void  invalidatePage(FILE* fp, int page_Num) override;
    BufStats getStats()   override;
    void     clearStats() override;
    std::vector<FrameInfo> getSnapshot()              override;
    void copyFramesFrom(ReplacementPolicy* other)     override;
};

// -------------------------------------------------------
// MRU
// -------------------------------------------------------
class MRUBufferManager : public ReplacementPolicy {
private:
    int num_Frames;
    std::list<Frame*> mru;
    std::unordered_map<std::pair<FILE*,int>,
                       std::list<Frame*>::iterator, PairHash> mp;
    BufStats stats;
public:
    explicit MRUBufferManager(int num_Frames);
    ~MRUBufferManager() override;
    char* getPage       (FILE* fp, int page_Num) override;
    void  unpinPage     (FILE* fp, int page_Num) override;
    void  invalidatePage(FILE* fp, int page_Num) override;
    BufStats getStats()   override;
    void     clearStats() override;
    std::vector<FrameInfo> getSnapshot()              override;
    void copyFramesFrom(ReplacementPolicy* other)     override;
};

#endif // __BUF_MANAGER_H_