//
//  defines.h
//  BigData
//
//  Created by Eric on 2015-12-22.
//  Copyright © 2015 eric. All rights reserved.
//

#ifndef defines_h
#define defines_h

#include <stdint.h>
#include <stdio.h>

#include <string.h>

#include <tbb/scalable_allocator.h>
#include "xxHash/xxhash.h"
#include "flat_hash_map.hpp"


#define PIXEL_JSON_FILE "/Volumes/ROOT_B/data.json"
#define USERAGENT_REGEXES_FILE "/Users/eric/git/regexes.yaml"
#define GEO_IP_FILE "/Users/eric/git/ipv4_addresses.csv"

typedef uint32_t small_key;
typedef uint64_t hash_key;

typedef __uint128_t uint128_t;

// set work size to 32megs
#define MAX_WORK_SIZE 1<<25

/* 
 memory chunk size - some memory chunks may not be completely filled requiring
  coalescing as a final step.  This should be divisible by the size of record
  struct.
*/
#define MEMORY_CHUNK_SIZE 64 << 14

struct HashPolicy
{
    uint64_t operator()(uint64_t v) const
    {
        return v;
    }
    typedef ska::power_of_two_hash_policy hash_policy;
};

typedef ska::flat_hash_map<uint64_t, const char *, HashPolicy, std::equal_to<uint64_t>> StringMap;


/**
 A Hash Map containing strings by 64-bit hash using a reentrant allocator

typedef google::dense_hash_map<
    hash_key,
    char *,
    std::hash<hash_key>,
    std::equal_to<hash_key>,
    tbb::scalable_allocator<std::pair<const hash_key, char*>>> StringMap;
 */


#endif /* defines_h */
