#include "bufferManager.hpp"

// --- BufStats Implementation ---
BufStats::BufStats() : accesses(0), diskreads(0), pageHits(0) {}

void BufStats::clear() {
    accesses = 0;
    diskreads = 0;
    pageHits = 0;
}

// --- Frame Implementation ---
Frame::Frame() : page_Num(-1), fp(nullptr), pin_count(0), second_chance(false), is_dirty(false) {
    page_Data = new char[PAGE_SIZE]; // Crucial: Allocate memory once per frame
}

Frame::~Frame() {
    delete[] page_Data;
}

void Frame::setFrame(FILE* file_ptr, int p_Num, bool is_pinned) {
    this->fp = file_ptr;
    this->page_Num = p_Num;
    this->pin_count = is_pinned ? 1 : 0;
    this->second_chance = true;
    this->is_dirty = false;
    
    if (fp != nullptr) {
        fseek(fp, page_Num * PAGE_SIZE, SEEK_SET);
        fread(page_Data, PAGE_SIZE, 1, fp);
    }
}

void Frame::pinFrame() {
    pin_count++;
}

void Frame::unpinFrame() {
    if (pin_count > 0) pin_count--;
}


// --- LRUBufferManager Implementation ---
LRUBufferManager::LRUBufferManager(int num_Frames) : num_Frames(num_Frames) {}

LRUBufferManager::~LRUBufferManager() {
    for (Frame* f : lru) {
        delete f;
    }
    lru.clear();
    mp.clear();
}

char* LRUBufferManager::getPage(FILE* fp, int page_Num) {
    stats.accesses++;

    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        stats.pageHits++;
        // O(1) update: Move existing frame to front without copying memory
        lru.splice(lru.begin(), lru, it->second);
        (*lru.begin())->pinFrame();
        return (*lru.begin())->page_Data;
    }

    stats.diskreads++;
    Frame* newFrame = nullptr;

    if ((int)lru.size() == num_Frames) {
        // Find the least recently used UNPINNED page (search from back)
        auto rev_it = lru.end();
        rev_it--;
        while (rev_it != lru.begin() && (*rev_it)->pin_count > 0) {
            rev_it--;
        }

        if ((*rev_it)->pin_count > 0) {
            return nullptr; // All pages are pinned, buffer is stalled
        }

        newFrame = *rev_it;
        mp.erase({newFrame->fp, newFrame->page_Num});
        lru.erase(rev_it);
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
    if (it != mp.end()) {
        (*it->second)->unpinFrame();
    }
}

BufStats LRUBufferManager::getStats() { return stats; }
void LRUBufferManager::clearStats() { stats.clear(); }


// --- ClockBufferManager Implementation ---
ClockBufferManager::ClockBufferManager(int num_Frames) : num_Frames(num_Frames), clock_hand(0), num_Pages(0) {
    bufferPool = new Frame[num_Frames]; // Calls default constructor, allocating page_Data safely
}

ClockBufferManager::~ClockBufferManager() {
    delete[] bufferPool;
}

char* ClockBufferManager::getPage(FILE* fp, int page_Num) {
    stats.accesses++;

    for (int i = 0; i < num_Pages; ++i) {
        if (bufferPool[i].fp == fp && bufferPool[i].page_Num == page_Num) {
            stats.pageHits++;
            bufferPool[i].second_chance = true;
            bufferPool[i].pinFrame();
            return bufferPool[i].page_Data;
        }
    }

    stats.diskreads++;

    if (num_Pages < num_Frames) {
        bufferPool[num_Pages].setFrame(fp, page_Num, true);
        char* data = bufferPool[num_Pages].page_Data;
        num_Pages++;
        return data;
    }

    while (true) {
        if (bufferPool[clock_hand].pin_count > 0) {
            clock_hand = (clock_hand + 1) % num_Frames;
            continue;
        }
        if (bufferPool[clock_hand].second_chance) {
            bufferPool[clock_hand].second_chance = false;
            clock_hand = (clock_hand + 1) % num_Frames;
            continue;
        }

        // Evict and replace
        bufferPool[clock_hand].setFrame(fp, page_Num, true);
        int st = clock_hand;
        clock_hand = (clock_hand + 1) % num_Frames;
        return bufferPool[st].page_Data;
    }
}

void ClockBufferManager::unpinPage(FILE* fp, int page_Num) {
    for (int i = 0; i < num_Pages; ++i) {
        if (bufferPool[i].fp == fp && bufferPool[i].page_Num == page_Num) {
            bufferPool[i].unpinFrame();
            return;
        }
    }
}

BufStats ClockBufferManager::getStats() { return stats; }
void ClockBufferManager::clearStats() { stats.clear(); }


// --- MRUBufferManager Implementation ---
MRUBufferManager::MRUBufferManager(int num_Frames) : num_Frames(num_Frames) {}

MRUBufferManager::~MRUBufferManager() {
    for (Frame* f : mru) {
        delete f;
    }
    mru.clear();
    mp.clear();
}

char* MRUBufferManager::getPage(FILE* fp, int page_Num) {
    stats.accesses++;

    auto it = mp.find({fp, page_Num});
    if (it != mp.end()) {
        stats.pageHits++;
        // O(1) update to move to front
        mru.splice(mru.begin(), mru, it->second);
        (*mru.begin())->pinFrame();
        return (*mru.begin())->page_Data;
    }

    stats.diskreads++;
    Frame* newFrame = nullptr;

    if ((int)mru.size() == num_Frames) {
        // MRU evicts the MOST recently used unpinned page (search from front)
        auto it_mru = mru.begin();
        while (it_mru != mru.end() && (*it_mru)->pin_count > 0) {
            it_mru++;
        }

        if (it_mru == mru.end()) {
            return nullptr; // Buffer stalled
        }

        newFrame = *it_mru;
        mp.erase({newFrame->fp, newFrame->page_Num});
        mru.erase(it_mru);
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
    if (it != mp.end()) {
        (*it->second)->unpinFrame();
    }
}

BufStats MRUBufferManager::getStats() { return stats; }
void MRUBufferManager::clearStats() { stats.clear(); }