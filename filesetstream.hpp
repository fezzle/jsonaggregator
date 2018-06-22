//
//  filesetstream.hpp
//  BigData
//
//  Created by Eric on 2016-08-06.
//  Copyright © 2016 eric. All rights reserved.
//

#ifndef filesetstream_hpp
#define filesetstream_hpp

#include <stdio.h>
#include <string.h>
#include <vector>
#include <string>
#include <fstream>
#include <iterator>
#include <utility>

#include <boost/log/trivial.hpp>
#include <tbb/tbb.h>
#include <tbb/mutex.h>

#include "memorymap.hpp"
#include "arrayrange.hpp"

// 4megabyte read size
#define READ_BUFF_SIZE 1<<22


using namespace std;


size_t min(size_t a, size_t b) {
    return (a < b ? a : b);
}

class MemoryMapRange {
public:
    MemoryMappedFile *memorymap;
    char *start;
    char *end;
    MemoryMapRange() {}
    
    MemoryMapRange(MemoryMappedFile *memorymap, char *start, char *end) :
            memorymap(memorymap), start(start), end(end) {
        // nothing
    }
};


class FileSetStream;

class FileSetStreamIterator {
public:
    tbb::mutex get_next_mutex;
    vector<MemoryMappedFile *>::iterator memorymapitr;
    char *memorymap_position;
    char *memorymap_end;
    FileSetStream *filesetstream;
    vector<MemoryMapRange *> ranges;
    
    FileSetStreamIterator(FileSetStream *filesetstream);
    
    MemoryMapRange *get_next(MemoryMapRange *last=nullptr);
    
    ~FileSetStreamIterator() {
        for (auto itr : ranges) {
            delete itr;
        }
        ranges.clear();
    }
};

class FileSetStream {
public:
    vector<MemoryMappedFile *> memorymaps;
    const vector<string> *inputfiles;
    char record_separator;
    int current_input_file;
    
    int current_memorymap;
    char *current_pos;
    
    FileSetStream(
            const vector<string> &inputfiles, char record_separator) :
            inputfiles(&inputfiles), record_separator(record_separator),
            current_input_file(0), current_memorymap(-1), current_pos(nullptr) {
        for (auto itr=inputfiles.begin(); itr!=inputfiles.end(); itr++) {
            MemoryMappedFile *mm = new MemoryMappedFile(*itr);
            memorymaps.push_back(mm);
        }
    }
    
    FileSetStreamIterator *get_itr() {
        return new FileSetStreamIterator(this);
    }
    
    ~FileSetStream() {
        for (auto itr : memorymaps) {
            delete itr;
        }
        memorymaps.clear();
    }
};


FileSetStreamIterator::FileSetStreamIterator(FileSetStream *filesetstream) :
        filesetstream(filesetstream),
        memorymapitr(filesetstream->memorymaps.begin()),
        memorymap_position(nullptr),
        memorymap_end(nullptr) {
    // nothing
}


MemoryMapRange *FileSetStreamIterator::get_next(MemoryMapRange *last) {
    tbb::mutex::scoped_lock lock(get_next_mutex);
    
    MemoryMappedFile *next_memorymap = *memorymapitr;
    char *next_start;
    char *next_end;
    
    if (memorymapitr == filesetstream->memorymaps.end()) {
        return nullptr;
    }
    
    if (memorymap_position == nullptr) {
        next_memorymap->grab();
        memorymap_position = next_memorymap->get_start();
        memorymap_end = next_memorymap->get_end();
        
    } else if (memorymap_position >= (*memorymapitr)->get_end()) {
        memorymapitr++;
        if (last != nullptr) {
            last->memorymap->release();
        }
        if (memorymapitr == filesetstream->memorymaps.end()) {
            return nullptr;
        }
            
        next_memorymap = *memorymapitr;
        next_memorymap->grab();
        memorymap_position = next_memorymap->get_start();
        memorymap_end = next_memorymap->get_end();
    }
        
    next_start = memorymap_position;
    next_end = &next_start[READ_BUFF_SIZE];
    if (next_end > memorymap_end) {
        next_end = memorymap_end;
    }
    
    if (last == nullptr) {
        last = new MemoryMapRange();
        ranges.push_back(last);
    }
    
    last->memorymap = next_memorymap;
    last->start = next_start;
    last->end = next_end;
    return last;
}
    


#endif /* filesetstream_hpp */
