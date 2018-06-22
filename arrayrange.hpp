//
//  arrayrange.hpp
//  QueryAgg
//
//  Created by Eric on 2017-02-01.
//  Copyright © 2017 norg. All rights reserved.
//

#ifndef arrayrange_hpp
#define arrayrange_hpp

#include <stdio.h>
#include <tbb/tbb.h>

#include <utility>


/* Chunk size is 4mb */
#define CHUNK_SIZE 1<<22


using namespace std;

template<class T, class C>
class ArrayRangeIterator {
public:
    T *start;
    T *end;
    T *position;
    C *container;
    tbb::mutex get_next_mutex;
    
    typedef pair<T*, T*> ArrayRange;

    ArrayRangeIterator(T *start, T *end, C *container) :
            start(start), position(start), end(end), container(container) {
        // nothing
    }
    
    ArrayRange get_next() {
        tbb::mutex::scoped_lock lock(get_next_mutex);
        
        if (position >= end && !container->advance(this)) {
            return ArrayRange(nullptr, nullptr);
        }
        
        T *startpos = position;
        T *endpos = startpos + CHUNK_SIZE;
        if (endpos > end) {
            endpos = end;
        }
        
        position = endpos;
        return ArrayRange(startpos, endpos);
    }
};


#endif /* arrayrange_hpp */
