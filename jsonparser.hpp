//
//  jsonparser.hpp
//  BigData
//
//  Created by Eric on 2016-01-01.
//  Copyright © 2016 eric. All rights reserved.
//

#ifndef jsonparser_hpp
#define jsonparser_hpp

#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <tbb/concurrent_queue.h>
#include <utility>

#include "defines.h"
#include "xxHash/xxhash.h"

#include "memorymap.hpp"
#include "texthandling.hpp"
#include "memorymanager.hpp"
#include "filesetstream.hpp"
#include "stringindex.hpp"

using namespace std;


template<typename JSONCLASS, typename SCRATCH>
class JsonParser {
    /**
     A functor which parses the supplied BufferSegment into the supplied
     MemoryManager's array of JSONCLASS.
     
     This class assumes it is thread local and provides a `merge()` method
     to combine state with JsonParser instances used in other threads.
     */
public:
    FileSetStreamIterator *inputstream;
    StringIndex *stringindex;
    MemoryManagerIterator<JSONCLASS> *outputstream;
    
    JsonParser(
            StringIndex *stringindex,
            FileSetStreamIterator *inputstream,
            MemoryManagerIterator<JSONCLASS> *outputstream):
                stringindex(stringindex),
                inputstream(inputstream),
                outputstream(outputstream)
        {
    }
    
//    static hash_key hash_key(char **data) {
//        /**
//         * Reads the next quote-surrounded string from *data and returns the
//         * hash.  *data is updated to the end of the string that was read.
//         */
//        char *keystart = *data;
//        char *keyend = keystart;
//        do {
//            keyend = strchr(keyend, '"');
//        } while (keyend[-1] == '\\');
//        *data = keyend + 1;
//        return XXH64(keystart, (keyend-keystart), 0);
//    }
    
    void operator()() const {
        go();
    }
    
    /**
     * Iterates over inputstream outputing parsed json structures to 
     *  outputstream
     */
    void go() const {
        MemoryMapRange *range = nullptr;
        char *input_pos = nullptr;
        char *input_end = nullptr;

        JSONCLASS *output_pos = nullptr;
        JSONCLASS *output_end = nullptr;
        while (true) {
            if (input_pos >= input_end) {
                range = inputstream->get_next(range);
                if (range->start != nullptr) {
                    input_pos = range->start;
                    input_end = range->end;
                } else {
                    bzero((void*)output_pos,
                        (size_t)output_end - (size_t(output_pos)));
                }
            }
            
            if (output_pos >= output_end) {
                pair<JSONCLASS *, JSONCLASS *> range = outputstream->get_next();
                if (range.first != nullptr) {
                    output_pos = range.first;
                    output_end = range.second;
                } else {
                    BOOST_LOG_TRIVIAL(error) << "no more output space" << endl;
                    break;
                }
            }
            
            while (input_pos < input_end && output_pos < output_end) {
                parse_json(&input_pos, input_end, output_pos);
                output_pos++;
            }
        }
    }
    
    JSONCLASS *parse_json(char **start, char *end, JSONCLASS *obj) const {
        // find open quote
        char *data = *start;
        SCRATCH scratch;
        
        char *next_eol = (char*)memchr(data, '\n', end-data);
        if (next_eol == nullptr) {
            next_eol = end;
        }
        
        // find first open bracket for object
        data = (char*)memchr(data, '{', end-data);
        
        while (data < next_eol) {
            // skip to first key, or break if newline
            data = (char *)memchr(data, '"', next_eol-data);
            data++;
            char *keystart = data;

            do { data = strchr(data, '"'); } while (data[-1] == '\\');
            char *keyend = data;
            
            // parse value
            while (*data == ':' || *data == ' ') {
                data++;
            }
            
            if (*data == '{') {
                data++;
                continue;
            }
            
            char *valstart = data;
            if (*data == '"') {
                data++;
                do {
                    data = (char*)memchr(data, '"', next_eol-data);
                } while (data && data[-1] == '\\');
            } else {
                data = (char*)memchr(data, ',', next_eol-data);
            }
            if (data == nullptr) {
                data = next_eol;
            }
            char *valend = data;

            obj->handle_keyvalue(
                keystart, keyend, valstart, valend, stringindex, &scratch);
            
            data = valend + 1;
        }
        data = next_eol+1;
        *start = data;
        //obj->
        return obj;
    }
};


#endif /* jsonparser_hpp */
