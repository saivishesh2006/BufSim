#include "bufferManager.hpp"

// constructor for Frame
Frame::Frame(){}

// copy constructor for Frame
Frame::Frame(const Frame &frame){
    this->page_Num = frame.page_Num;
    this->page_Data = new char[PAGE_SIZE];
    memcpy(this->page_Data, frame.page_Data, PAGE_SIZE);
    this->fp = frame.fp;
    this->pinned = frame.pinned;
    this->second_chance = frame.second_chance;
}

// populates a frame in Memory
void Frame::setFrame(FILE*fp, int page_Num, char* page_Data, bool pinned){
    this->page_Num = page_Num;
    this->page_Data = page_Data;
    this->fp = fp;
    this->pinned = pinned;
    this->second_chance = true;
}

// unpin a frame
void Frame::unpinFrame(){
    this->pinned = false;
}

// destructor for Frame
Frame::~Frame(){
    delete[] page_Data;
}


// constructor for LRUBufferManager
LRUBufferManager::LRUBufferManager(int num_Frames): num_Frames(num_Frames) {}

// destructor for LRUBufferManager
LRUBufferManager::~LRUBufferManager(){
    lru.clear();    // calls destructor of Frame so delete of pageData happens
    mp.clear();
}

// get a page from buffer
char* LRUBufferManager::getPage(FILE*fp, int page_Num){

    // check if page present in memory using map
    auto it = mp.find({fp, page_Num});
    if(it!=mp.end()){
        //check later
        stats.accesses+=1;
        stats.pageHits+=1;
        // page present in memory
        lru.push_front(*it->second);
        lru.erase(it->second);
        mp[{fp, page_Num}] = lru.begin();
        lru.begin()->pinned = true;
        return lru.begin()->page_Data;
    }
    // if page is not in memory
    // check if space is there in buffer

    if((int)lru.size() == num_Frames){

        // find last unpinned page and remove it
        auto it = lru.end();
        it--;
        while(it->pinned){
            if(it==lru.begin())return NULL;
            it--;
        }
        // remove page from buffer
        mp.erase({it->fp, it->page_Num});
        lru.erase(it);
    }

    // add the page to buffer
    char* page_Data = new char[PAGE_SIZE];
    fseek(fp, page_Num*PAGE_SIZE, SEEK_SET);
    fread(page_Data, PAGE_SIZE, 1, fp);

    Frame frame = Frame();
    frame.setFrame(fp, page_Num, page_Data, true);
    lru.push_front(frame);

    mp[{fp, page_Num}] = lru.begin();
    stats.accesses++;
    stats.diskreads++;
    char name[20];
    memcpy(name, page_Data, 20);
    return lru.begin()->page_Data;
}

// clear stats
void LRUBufferManager::clearStats(){
    stats.clear();
}

// get stats
BufStats LRUBufferManager::getStats(){
    return stats;
}

// constructor for BufStats
BufStats::BufStats(): accesses(0), diskreads(0), pageHits(0) {}

// clear stats
void BufStats::clear(){
    accesses = 0;
    diskreads = 0;
    pageHits = 0;
}


// unpin a page
void LRUBufferManager::unpinPage(FILE*fp, int page_Num){
    // check if page present in memory using map
    auto it = mp.find({fp, page_Num});
    if(it != mp.end()){
        // page present in memory
        // unpin page
        it->second->unpinFrame();
    }
}

// constructor for ClockBufferManager
ClockBufferManager::ClockBufferManager(int num_Frames): num_Frames(num_Frames), clock_hand(0), num_Pages(0){
    bufferPool = new Frame[num_Frames];
}

// destructor for ClockBufferManager
ClockBufferManager::~ClockBufferManager(){
    delete[] bufferPool;
}

// get a page from buffer
char *ClockBufferManager::getPage(FILE* fp, int page_Num){


    // check if the page is present in memory
    for(int i=0;i<num_Pages;++i){
        if(bufferPool[i].fp == fp && bufferPool[i].page_Num == page_Num){
            // page is present in memory
            // update stats
            stats.accesses++;
            // update second chance
            bufferPool[i].second_chance = true;
            bufferPool[i].pinned = true;
            stats.pageHits++;
            return bufferPool[i].page_Data;
        }
    }

    // page is not present in memory
    if(num_Pages < num_Frames){
        fseek(fp, page_Num*PAGE_SIZE, SEEK_SET);
        char* page_Data = new char[PAGE_SIZE];
        fread(page_Data, PAGE_SIZE, 1, fp);
        bufferPool[num_Pages].setFrame(fp, page_Num, page_Data, true);
        num_Pages++;
        stats.accesses++;
        stats.diskreads++;
        return page_Data;
    }
    stats.accesses++;
    // page is not present in memory and memory is full
    while(true){
        if(bufferPool[clock_hand].second_chance){
            // page has second chance
            bufferPool[clock_hand].second_chance = false;
            clock_hand = (clock_hand+1)%num_Frames;
            continue;
        }
        if(bufferPool[clock_hand].pinned){
            // page is pinned
            clock_hand = (clock_hand+1)%num_Frames;
            continue;
        }
        // page is not pinned and does not have second chance
        // seek the page in file
        fseek(fp, page_Num*PAGE_SIZE, SEEK_SET);
        fread(bufferPool[clock_hand].page_Data, PAGE_SIZE, 1, fp);
        bufferPool[clock_hand].fp = fp;
        bufferPool[clock_hand].page_Num = page_Num;
        bufferPool[clock_hand].pinned = true;
        bufferPool[clock_hand].second_chance = true;
        int st = clock_hand;
        clock_hand = (clock_hand+1)%num_Frames;
        stats.diskreads++;
        return bufferPool[st].page_Data;
    }
}

// unpin a page
void ClockBufferManager::unpinPage(FILE* fp, int page_Num){
    
    // check if page is present in memory
    for(int i=0;i<num_Pages;++i){
        if(bufferPool[i].fp == fp && bufferPool[i].page_Num == page_Num){
            // page is present in memory
            // unpin page
            bufferPool[i].unpinFrame();
            return;
        }
    }
}

// clear stats
void ClockBufferManager::clearStats(){
    stats.clear();
}

// get stats
BufStats ClockBufferManager::getStats(){
    return stats;
}

// constructor for MRUBufferManager
MRUBufferManager::MRUBufferManager(int num_Frames): num_Frames(num_Frames) {}

// destructor for MRUBufferManager
MRUBufferManager::~MRUBufferManager(){
    mru.clear();      // the destructor of frame will be automatically called
    mp.clear();
}

// get a page from buffer
char *MRUBufferManager::getPage(FILE* fp, int page_Num){

    // check if page is present in memory
    auto it = mp.find({fp, page_Num});
    if(it!=mp.end()){
        stats.accesses++;
        // present so bring it to first and pin
        mru.push_front(*it->second);
        mru.erase(it->second);
        mp[{fp, page_Num}] = mru.begin();
        mru.begin()->pinned = true;
        stats.pageHits++;
        return mru.begin()->page_Data;
    }

    // not in memory, so check size
    if((int)mru.size() == num_Frames){

        int rmv = 0;
        for(auto it=mru.begin();it!=mru.end();++it){
            if(it->pinned){
                // page is pinned
                continue;
            }
            // page is not pinned
            // remove it from memory
            mp.erase({it->fp, it->page_Num});
            mru.erase(it);
            rmv = 1;
            break;
        }

        if(!rmv)return NULL;
    }

    // add the frame at start
    fseek(fp, page_Num*PAGE_SIZE, SEEK_SET);
    char* page_Data = new char[PAGE_SIZE];
    fread(page_Data, PAGE_SIZE, 1, fp);
    Frame frame = Frame();
        
    frame.setFrame(fp, page_Num, page_Data, true);
    mru.push_front(frame);
    mp[{fp, page_Num}] = mru.begin();
    stats.accesses++;
    stats.diskreads++;
    return mru.begin()->page_Data;
}


// unpin the frame

void MRUBufferManager::unpinPage(FILE *fp, int page_Num){

    // check if page is present in memory
    auto it = mp.find({fp, page_Num});
    if(it!=mp.end()){
        // page is present in memory
        // unpin page
        it->second->unpinFrame();
    }
}

void MRUBufferManager::clearStats(){
    stats.clear();
}

BufStats MRUBufferManager::getStats(){
    return stats;
}
