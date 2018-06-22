//
//  memorymanager.hpp
//  BigData
//
//  Created by Eric on 2016-01-13.
//  Copyright © 2016 eric. All rights reserved.
//

#ifndef memorymanager_hpp
#define memorymanager_hpp


#include <stdio.h>

#include <vector>
#include <utility>

#include <boost/log/trivial.hpp>
#include <boost/assert.hpp>

#include <tbb/mutex.h>

#include "defines.h"
#include "arrayrange.hpp"

using namespace std;

template<class T> class MemoryManager;

template<class T>
class MemoryManagerIterator {
public:
    T *position;
    MemoryManager<T> *memorymanager;
    tbb::mutex get_next_mutex;
    
    MemoryManagerIterator(MemoryManager<T> *memorymanager) :
            position(memorymanager->start),
            memorymanager(memorymanager) {
        // nothing
    }
    
    T* get_start() {
        return memorymanager->start;
    }
    
    T* get_end() {
        return memorymanager->end;
    }
    
    pair<T*, T*> get_next() {
        // critical section
        tbb::mutex::scoped_lock lock(get_next_mutex);
        
        if (position >= memorymanager->end) {
            return make_pair<T*, T*>(nullptr, nullptr);
        }
        
        T *first = position;
        T *second = position + memorymanager->chunk_size;
        if (second > memorymanager->end) {
            second = memorymanager->end;
        }
        position = second;
        
        return make_pair(first, second);
    }
};


template<class T>
class MemoryManager {
private:
    // no copies, moves, assigns
    MemoryManager(MemoryManager const&) = delete;
    void operator=(MemoryManager const&) = delete;
    MemoryManager() = delete;
    MemoryManager(MemoryManager&&) = delete;
    MemoryManager& operator=(MemoryManager&&) = delete;
    
public:
    
    T *start;
    T *end;
    
    size_t chunk_size;
    size_t count;
    
    MemoryManager(size_t count, size_t chunk_size)
            :   count(count),
                chunk_size(chunk_size) {
        /**
         @param total_size: Number of T records to alloc space for
         @param chunk_size: The number of T records that should fit in the space
         returned by @see alloc
         */
        size_t total_size = (count * sizeof(T));
        BOOST_LOG_TRIVIAL(info) << "MemoryManager: Allocating space for "
                                << count << " of " << sizeof(T)
                                << "B records. Allocing: " << total_size;
        start = (T*)malloc(total_size);
        end = &start[count];
    };
    
    T *get_start() {
        return this->start;
    }
    
    T *get_end() {
        return this->end;
    }
    
    MemoryManagerIterator<T> *get_itr() {
        return new MemoryManagerIterator<T>(this);
    }
    
    void clear() {
        if (start) {
            free(start);
            start = nullptr;
        }
    }
    
    ~MemoryManager<T>() {
        clear();
    }
};


#endif /* memorymanager_hpp */
