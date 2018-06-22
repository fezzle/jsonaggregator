//
//  stringindex.cpp
//  QueryAgg
//
//  Created by Eric on 2017-01-25.
//  Copyright © 2017 norg. All rights reserved.
//

#include "stringindex.hpp"

#include <vector>
#include <algorithm>


std::vector<StringIndex*> StringIndex::stringindexes;


StringIndex *StringIndex::new_instance() {
    StringIndex *newindex = new StringIndex();
    stringindexes.push_back(newindex);
    return newindex;
}


void StringIndex::combine(StringIndex &a, StringIndex &b) {
    for (auto itr=b.stringmap.begin(); itr!=b.stringmap.end(); itr++) {
        a.set((*itr).first, (*itr).second);
    }
    b.stringmap.clear();
    b.strings.clear();
}


StringIndex::StringIndex(const StringIndex &obj) {
    stringmap.set_empty_key((hash_key)0);
}


StringIndex::StringIndex() {
    stringmap.set_empty_key((hash_key)0);
};

StringIndex *StringIndex::combine_all() {
    if (stringindexes.size() == 0) {
        return nullptr;
    }
    if (stringindexes.size() == 1) {
        return stringindexes.front();
    }

    std::sort(stringindexes.begin(), stringindexes.end(),
        [](const StringIndex *a, const StringIndex *b) {
            return a->stringmap.size() > b->stringmap.size();
        }
    );
    
    auto stringindex_itr = stringindexes.begin();
    StringIndex *biggest_string_index = *stringindex_itr;
    
    while (++stringindex_itr != stringindexes.end()) {
        StringIndex::combine(*biggest_string_index, **stringindex_itr);
        delete *stringindex_itr;
    }
    stringindexes.empty();
    stringindexes.push_back(biggest_string_index);
    return biggest_string_index;
}