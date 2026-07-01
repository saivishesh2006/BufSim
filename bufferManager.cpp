#include "bufferManager.hpp"
#include <iostream>

// -------------------------------------------------------
// BufStats
// -------------------------------------------------------
BufStats::BufStats() : accesses(0), diskreads(0), pageHits(0) {}

void BufStats::clear() {
    accesses = 0;
    diskreads = 0;
    pageHits  = 0;
}

// -------------------------------------------------------
// Frame
// -------------------------------------------------------
Frame::Frame()
    : page_Num(-1), fp(nullptr), pin_count(0),
      second_chance(false), is_dirty(false)
{
    page_Data = new char[PAGE_SIZE];
}

Frame::~Frame() { delete[] page_Data; }

void Frame::setFrame(FILE* file_ptr, int p_Num, bool is_pinned) {
    fp           = file_ptr;
    page_Num     = p_Num;
    pin_count    = is_pinned ? 1 : 0;
    second_chance = true;
    is_dirty     = false;
    if (fp) {
        fseek(fp, (long)page_Num * PAGE_SIZE, SEEK_SET);
        size_t nr = fread(page_Data, PAGE_SIZE, 1, fp);
        if (nr == 0) memset(page_Data, 0, PAGE_SIZE); // new/short page
    }
}

void Frame::pinFrame()   { pin_count++; }
void Frame::unpinFrame() { if (pin_count > 0) pin_count--; }

// -------------------------------------------------------
// LRUBufferManager
// -------------------------------------------------------
LRUBufferManager::LRUBufferManager(int n) : num_Frames(n) {}

LRUBufferManager::~LRUBufferManager() {
    for (Frame* f : lru) delete f;
}

char* LRUBufferManager::getPage(FILE* fp, int page_Num) {
    stats.accesses++;
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        stats.pageHits++;
        lru.splice(lru.begin(), lru, it->second);
        it->second = lru.begin();
        (*lru.begin())->pinFrame();
        return (*lru.begin())->page_Data;
    }

    stats.diskreads++;
    Frame* newFrame = nullptr;

    if ((int)lru.size() == num_Frames) {
        auto rev = lru.end();
        --rev;
        while (rev != lru.begin() && (*rev)->pin_count > 0) --rev;
        if ((*rev)->pin_count > 0) return nullptr; // all pinned
        newFrame = *rev;
        mp.erase({newFrame->fp, newFrame->page_Num});
        lru.erase(rev);
    } else {
        newFrame = new Frame();
    }

    newFrame->setFrame(fp, page_Num, true);
    lru.push_front(newFrame);
    mp[{fp, page_Num}] = lru.begin();
    return newFrame->page_Data;
}

void LRUBufferManager::unpinPage(FILE* fp, int page_Num) {
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) (*it->second)->unpinFrame();
}

void LRUBufferManager::invalidatePage(FILE* fp, int page_Num) {
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        delete *(it->second);
        lru.erase(it->second);
        mp.erase(it);
    }
}

BufStats LRUBufferManager::getStats()   { return stats; }
void     LRUBufferManager::clearStats() { stats.clear(); }

std::vector<FrameInfo> LRUBufferManager::getSnapshot() {
    std::vector<FrameInfo> snap;
    int rank = 1;
    for (auto it = lru.begin(); it != lru.end(); ++it, ++rank) {
        FrameInfo fi;
        fi.fp           = (*it)->fp;
        fi.page_num     = (*it)->page_Num;
        fi.pin_count    = (*it)->pin_count;
        fi.recency_rank = rank;   // 1 = most recently used
        fi.occupied     = ((*it)->fp != nullptr);
        snap.push_back(fi);
    }
    return snap;
}

void LRUBufferManager::copyFramesFrom(ReplacementPolicy* other) {
    auto snap = other->getSnapshot();
    // Insert in reverse order so rank-1 (MRU) ends up at front
    for (auto it = snap.rbegin(); it != snap.rend(); ++it) {
        if (!it->occupied || it->fp == nullptr) continue;
        // Only add if not already present
        if (mp.find({it->fp, it->page_num}) != mp.end()) continue;
        if ((int)lru.size() == num_Frames) break;
        Frame* f = new Frame();
        f->fp        = it->fp;
        f->page_Num  = it->page_num;
        f->pin_count = 0;  // unpin on transfer
        // copy page data
        fseek(f->fp, (long)f->page_Num * PAGE_SIZE, SEEK_SET);
        size_t nr = fread(f->page_Data, PAGE_SIZE, 1, f->fp);
        if (nr == 0) memset(f->page_Data, 0, PAGE_SIZE);
        f->second_chance = false;
        f->is_dirty      = false;
        lru.push_front(f);
        mp[{f->fp, f->page_Num}] = lru.begin();
    }
}


// -------------------------------------------------------
// ClockBufferManager
// FIX 1: unordered_map for O(1) lookup (was O(n) linear scan)
// FIX 2: all-pinned escape so the eviction loop cannot hang
// -------------------------------------------------------
ClockBufferManager::ClockBufferManager(int n)
    : num_Frames(n), clock_hand(0), num_Pages(0)
{
    bufferPool = new Frame[num_Frames];
}

ClockBufferManager::~ClockBufferManager() { delete[] bufferPool; }

char* ClockBufferManager::getPage(FILE* fp, int page_Num) {
    stats.accesses++;

    // O(1) lookup via map
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        int idx = it->second;
        stats.pageHits++;
        bufferPool[idx].second_chance = true;
        bufferPool[idx].pinFrame();
        return bufferPool[idx].page_Data;
    }

    stats.diskreads++;

    // Pool not full yet — just append
    if (num_Pages < num_Frames) {
        int idx = num_Pages++;
        bufferPool[idx].setFrame(fp, page_Num, true);
        mp[{fp, page_Num}] = idx;
        return bufferPool[idx].page_Data;
    }

    // CLOCK sweep — FIX: count full rotations to detect all-pinned
    int scans = 0;
    while (scans < 2 * num_Frames) {
        int h = clock_hand;
        clock_hand = (clock_hand + 1) % num_Frames;

        if (bufferPool[h].pin_count > 0) { scans++; continue; }
        if (bufferPool[h].second_chance) {
            bufferPool[h].second_chance = false;
            scans++;
            continue;
        }
        // Evict frame h
        mp.erase({bufferPool[h].fp, bufferPool[h].page_Num});
        bufferPool[h].setFrame(fp, page_Num, true);
        mp[{fp, page_Num}] = h;
        return bufferPool[h].page_Data;
    }
    return nullptr; // all frames pinned
}

void ClockBufferManager::unpinPage(FILE* fp, int page_Num) {
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) bufferPool[it->second].unpinFrame();
}

void ClockBufferManager::invalidatePage(FILE* fp, int page_Num) {
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        int idx = it->second;
        // Clear the frame so it looks empty; keep it in the pool for reuse
        bufferPool[idx].fp       = nullptr;
        bufferPool[idx].page_Num = -1;
        bufferPool[idx].pin_count = 0;
        mp.erase(it);
    }
}

BufStats ClockBufferManager::getStats()   { return stats; }
void     ClockBufferManager::clearStats() { stats.clear(); }

std::vector<FrameInfo> ClockBufferManager::getSnapshot() {
    std::vector<FrameInfo> snap;
    for (int i = 0; i < num_Pages; ++i) {
        FrameInfo fi;
        fi.fp            = bufferPool[i].fp;
        fi.page_num      = bufferPool[i].page_Num;
        fi.pin_count     = bufferPool[i].pin_count;
        fi.second_chance = bufferPool[i].second_chance;
        fi.recency_rank  = i + 1;  // slot order (clock position)
        fi.occupied      = (bufferPool[i].fp != nullptr && bufferPool[i].page_Num >= 0);
        snap.push_back(fi);
    }
    return snap;
}

void ClockBufferManager::copyFramesFrom(ReplacementPolicy* other) {
    auto snap = other->getSnapshot();
    for (auto& fi : snap) {
        if (!fi.occupied || fi.fp == nullptr) continue;
        if (mp.find({fi.fp, fi.page_num}) != mp.end()) continue;
        if (num_Pages >= num_Frames) break;
        int idx = num_Pages++;
        bufferPool[idx].fp           = fi.fp;
        bufferPool[idx].page_Num     = fi.page_num;
        bufferPool[idx].pin_count    = 0;
        bufferPool[idx].second_chance = true;  // give second chance on arrival
        bufferPool[idx].is_dirty     = false;
        fseek(fi.fp, (long)fi.page_num * PAGE_SIZE, SEEK_SET);
        size_t nr = fread(bufferPool[idx].page_Data, PAGE_SIZE, 1, fi.fp);
        if (nr == 0) memset(bufferPool[idx].page_Data, 0, PAGE_SIZE);
        mp[{fi.fp, fi.page_num}] = idx;
    }
}


// -------------------------------------------------------
// MRUBufferManager
// -------------------------------------------------------
MRUBufferManager::MRUBufferManager(int n) : num_Frames(n) {}

MRUBufferManager::~MRUBufferManager() {
    for (Frame* f : mru) delete f;
}

char* MRUBufferManager::getPage(FILE* fp, int page_Num) {
    stats.accesses++;
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        stats.pageHits++;
        mru.splice(mru.begin(), mru, it->second);
        it->second = mru.begin();
        (*mru.begin())->pinFrame();
        return (*mru.begin())->page_Data;
    }

    stats.diskreads++;
    Frame* newFrame = nullptr;

    if ((int)mru.size() == num_Frames) {
        auto fwd = mru.begin();
        while (fwd != mru.end() && (*fwd)->pin_count > 0) ++fwd;
        if (fwd == mru.end()) return nullptr; // all pinned
        newFrame = *fwd;
        mp.erase({newFrame->fp, newFrame->page_Num});
        mru.erase(fwd);
    } else {
        newFrame = new Frame();
    }

    newFrame->setFrame(fp, page_Num, true);
    mru.push_front(newFrame);
    mp[{fp, page_Num}] = mru.begin();
    return newFrame->page_Data;
}

void MRUBufferManager::unpinPage(FILE* fp, int page_Num) {
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) (*it->second)->unpinFrame();
}

void MRUBufferManager::invalidatePage(FILE* fp, int page_Num) {
    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        delete *(it->second);
        mru.erase(it->second);
        mp.erase(it);
    }
}

BufStats MRUBufferManager::getStats()   { return stats; }
void     MRUBufferManager::clearStats() { stats.clear(); }

std::vector<FrameInfo> MRUBufferManager::getSnapshot() {
    std::vector<FrameInfo> snap;
    int rank = 1;
    for (auto it = mru.begin(); it != mru.end(); ++it, ++rank) {
        FrameInfo fi;
        fi.fp           = (*it)->fp;
        fi.page_num     = (*it)->page_Num;
        fi.pin_count    = (*it)->pin_count;
        fi.recency_rank = rank;  // 1 = most recently used (evict candidate for MRU)
        fi.occupied     = ((*it)->fp != nullptr);
        snap.push_back(fi);
    }
    return snap;
}

void MRUBufferManager::copyFramesFrom(ReplacementPolicy* other) {
    auto snap = other->getSnapshot();
    for (auto it = snap.rbegin(); it != snap.rend(); ++it) {
        if (!it->occupied || it->fp == nullptr) continue;
        if (mp.find({it->fp, it->page_num}) != mp.end()) continue;
        if ((int)mru.size() == num_Frames) break;
        Frame* f = new Frame();
        f->fp        = it->fp;
        f->page_Num  = it->page_num;
        f->pin_count = 0;
        fseek(f->fp, (long)f->page_Num * PAGE_SIZE, SEEK_SET);
        size_t nr = fread(f->page_Data, PAGE_SIZE, 1, f->fp);
        if (nr == 0) memset(f->page_Data, 0, PAGE_SIZE);
        f->second_chance = false;
        f->is_dirty      = false;
        mru.push_front(f);
        mp[{f->fp, f->page_Num}] = mru.begin();
    }
}