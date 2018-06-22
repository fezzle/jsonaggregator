//
//  stringindex.hpp
//  QueryAgg
//
//  Created by Eric on 2017-01-25.
//  Copyright © 2017 norg. All rights reserved.
//

#ifndef stringindex_hpp
#define stringindex_hpp

#include <stdio.h>
#include <strings.h>

#include <vector>

#include <boost/log/trivial.hpp>

#include "defines.h"
#include "xxhash.h"

/* 16 megabyte block size */
#define BLOCK_SIZE (1<<24)


using namespace std;


class Block {
public:
    uint8_t *start;
    Block *next;
    Block(uint8_t *start, Block *next) :
        start(start), next(next) {}
};


class MemoryBlock {
public:
    Block *current_block;
    uint8_t *current_block_position;
    uint8_t *current_block_end;

    MemoryBlock() : current_block(nullptr), current_block_position(nullptr) {
        add_block();
    }
    
    void add_block() {
        uint8_t *start_memory_block = (uint8_t*)malloc(BLOCK_SIZE);
        current_block = new ((Block*)start_memory_block)
            Block(start_memory_block, current_block);
    
        current_block_position = current_block->start + sizeof(Block);
        current_block_end = &start_memory_block[BLOCK_SIZE];
    }
    
    void *alloc(size_t size) {
        if (&current_block_position[size] > current_block_end) {
            add_block();
        }
        void *alloced_area = current_block_position;
        current_block_position += size;
        return alloced_area;
    }
    
    size_t size() {
        size_t acc = 0;
        for (Block *itr=current_block; itr != nullptr; itr = itr->next) {
            if (itr == current_block) {
                acc += current_block_position - itr->start;
            } else {
                acc += BLOCK_SIZE;
            }
        }
        return acc;
    }
    
    size_t max_size() {
        return BLOCK_SIZE - sizeof(Block);
    }
    
    
    void clear() {
        Block *itr;
        while (nullptr != (itr=current_block)) {
            current_block = itr->next;
            free(itr->start);
        }
    }
    
    ~MemoryBlock() {
        clear();
    }
};


class StringIndex {
private:
    StringIndex(const StringIndex &obj);
    StringIndex();

public:
    static vector<StringIndex*> stringindexes;
    StringMap stringmap;
    MemoryBlock strings;
    
    hash_key put(const char *start, size_t len) {
        hash_key key = XXH64(start, len, 0);
        set(key, start, len);
        return key;
    }
    
    hash_key set(hash_key key, const char *val, size_t val_len=-1) {
        if (stringmap.find(key) == stringmap.end()) {
            if (val_len == -1) {
                val_len = strlen(val);
            }
            char *new_val_mem = (char*)strings.alloc(val_len + 1);
            memcpy(new_val_mem, val, val_len);
            new_val_mem[val_len] = '\0';
            stringmap[key] = new_val_mem;
        }
        return key;
    }
    
    hash_key set_compound(size_t count, ...) {
        /**
         * Sets a compound key
         * @param count The number of strings being supplied
         * @param ... [[char *str, size_t str_len], ....] The strings in key
         */
        va_list arguments, arguments2;
        
        va_start(arguments, count);
        va_copy(arguments2, arguments);
        
        hash_key key = 0;
        size_t compound_value_length = 0;
        for (int i = 0; i < count; i += 2) {
            char *str = va_arg(arguments, char *);
            size_t str_len = va_arg(arguments, size_t);
            key = XXH64(str, str_len, key);
            compound_value_length += va_arg(arguments, size_t);
        }
        
        if (stringmap.find(key) == stringmap.end()) {
            char *new_val_mem = (char*)strings.alloc(
                compound_value_length + count + 1);
            char *mem_itr = new_val_mem;
            for (int i = 0; i < count; i += 2) {
                char *str = va_arg(arguments2, char *);
                size_t str_len = va_arg(arguments2, size_t);
                memcpy(mem_itr, str, str_len);
                mem_itr += str_len;
                *mem_itr++ = ':';
            }
            *mem_itr = '\0';
            stringmap[key] = new_val_mem;
        }
       
        va_end(arguments);
        va_end(arguments2);
        return key;
    }
    
    char *get(hash_key key) {
        auto itr = stringmap.find(key);
        if (itr == stringmap.end()) {
            return nullptr;
        } else {
            return itr->second;
        }
    }
    
    static StringIndex *combine_all();
    static void combine(StringIndex &a, StringIndex &b);
    static StringIndex *new_instance();
};


#endif /* stringindex_hpp */
