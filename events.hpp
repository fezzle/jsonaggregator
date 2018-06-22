//
//  events.hpp
//  BigData
//
//  Created by Eric on 2015-12-31.
//  Copyright © 2015 eric. All rights reserved.
//

#ifndef events_hpp
#define events_hpp

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utility>
#include <vector>
#include <string>
#include <sstream>
#include <tbb/scalable_allocator.h>
#include <tbb/concurrent_queue.h>
#include <boost/assert.hpp>

#include "defines.h"
#include "flat_hash_map.hpp"
#include "xxhash.h"
#include "ua_resolver.hpp"
#include "geoip.hpp"
#include "texthandling.hpp"
#include "stringindex.hpp"

using namespace std;

class Event;
class EventScratch;
class ThreadLocals;



/**
 Method prototype for methods handling a (json)key value.
 */
typedef void (Event::*keyhandler_fn)(
    char * const,
    char * const,
    StringIndex * const,
    EventScratch * const);



template<typename VALUE, VALUE emptyval>
class Djb2Map {
    /**
    A Djb2Map based hash function.
    
    It breaks if there is a string collision with DJB.
    */
    size_t size;
    size_t rehash_count;
    
    typedef size_t key_t;
    
    typedef pair<key_t, VALUE> table_element_t;
    
    // this is necessary to assert-fail on djb2-hash collision
    typedef pair<string, key_t> key_hash_pair_t;
    
    table_element_t *table;
    vector<key_hash_pair_t> mappings;
    
public:
    Djb2Map() : size(31), rehash_count(0) {
        table = new table_element_t[this->size];
    }
    
    void rehash(size_t new_size) {
        this->rehash_count++;
        
        // create empty new table
        table_element_t *new_table = new table_element_t[new_size];
        for (size_t i=0; i<new_size; i++) {
            new_table[i].first = 0;
        }
        
        // copy old values to new values
        for (size_t i=0; i<size; i++) {
            if (this->table[i].first != 0) {
                size_t new_table_i = this->table[i].first % new_size;
                if (new_table[new_table_i].first != 0) {
                    // collision copying to new table.  try rehashing again
                    free(new_table);
                    rehash(new_size + 3);
                    return;
                } else {
                    new_table[new_table_i] = this->table[i];
                }
            }
        }
        free(this->table);
        this->size = new_size;
        this->table = new_table;
    }
    
    key_t hash(const char *start, const char *end) {
        key_t hash = 5381;
        while (start != end) {
            hash = ((hash << 5) + hash) ^ (*start); /* hash * 33 + c */
            start++;
        }
        return hash;
    }

    void set(const char *start, const char *end, VALUE value) {
        string key_string((char*)start, (size_t)(end-start));
        key_t hashkey = this->hash(start, end);
        // verify no collision
        for (auto itr=mappings.begin(); itr!=mappings.end(); itr++) {
            if ((*itr).second == hashkey) {
                // if hashkeys match, assert value matches
                BOOST_ASSERT_MSG(
                    (*itr).first != key_string,
                    (ostringstream() << "DJB2 Unrecoverable Key Collision: "
                        << (*itr).first << ", " << key_string).str());
            }
        }
        mappings.push_back(key_hash_pair_t(key_string, hashkey));
        
        while (true) {
            size_t i = hashkey % this->size;
            if (this->table[i].first != 0) {
                // collision, rehash and try again
                this->rehash(this->size + 3);
                continue;
            } else {
                this->table[i].first = hashkey;
                this->table[i].second = value;
                break;
            }
        }
    };
    
    VALUE get(char *start, char *end) {
        size_t i = this->hash(start, end) % this->size;
        if (this->table[i].first != 0) {
            return this->table[i].second;
        } else {
            return emptyval;
        }
    }
};



/**
 Enumeration of known event types
 */
enum EventType {
    UNKNOWN, PING, IMPR, PGV2, ADREQ, ADRESP
};


class EventScratch {
public:
    char * sesn;
    size_t sesn_len;
    char * vic;
    size_t vic_len;
    char * user;
    size_t user_len;
    EventScratch() : sesn(NULL), vic(NULL), user(NULL) {}
};



class Event {
public:
    static Djb2Map<keyhandler_fn, (keyhandler_fn)NULL> key_map;
    
    typedef EventScratch EventScratch;
   
    static void bind(const char *str, keyhandler_fn set_fn) {
        /**
         Binds the string *str to the supplied handler fn.
         */
        key_map.set((char*)str, (char*)&str[strlen(str)], set_fn);
    }
    
    static void static_init() {
        /**
         Binds json keys to handler methods 
         */
        //Event::key_hashes.set_empty_key((hash_key)0);
        Event::bind("cs_user_agent", &Event::set_browser);
        Event::bind("property", &Event::set_property);
        Event::bind("eiv_vic", &Event::set_vic);
        Event::bind("eiv_usr", &Event::set_user);
        Event::bind("cu", &Event::set_creative);
        Event::bind("timestamp", &Event::set_timestamp);
        Event::bind("timespent", &Event::set_timespent);
        Event::bind("etype", &Event::set_event_type);
        Event::bind("client_type", &Event::set_client_type);
        Event::bind("eiv_sesn", &Event::set_session);
        Event::bind("eiv_evt", &Event::set_event_id);
        Event::bind("c_ip", &Event::set_ip);
        Event::bind("w", &Event::set_placementlabel);
    }
    

    /* 32 bytes */
    hash_key property_key;
    hash_key creative_key;
    hash_key placementlabel_key;
    hash_key vic_user_sesn_ident;

    /* 16 bytes */
    small_key devicenplatform_key;
    uint32_t ip_value;
    uint32_t event_id;
    uint32_t event_type;
    uint32_t client_type;
    uint32_t timestamp;
    uint32_t timespent;
    uint32_t padding;
    
    Event() {}
    
    void set_session(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        scratch->sesn = startpos;
        scratch->sesn_len = endpos - startpos;
    }
    
    void set_event_id(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        event_id = (uint32_t)read_number(startpos, endpos);
    }
    
    void set_placementlabel(
            char * const startpos,
            char * const endpos,
            StringIndex *stringindex,
            EventScratch * const scratch) {
        placementlabel_key = stringindex->put(startpos, endpos-startpos);
    }
    
    void set_property(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        property_key = stringindex->put(startpos, endpos-startpos);
    }
    
    void set_vic(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        scratch->vic = startpos;
        scratch->vic_len = endpos - startpos;
    }
    
    void set_ip(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        char *ptr = startpos;
        ip_value = read_ip(&ptr);
    }
    
    void set_creative(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        creative_key = stringindex->put(startpos, endpos-startpos);
    }
    
    void set_browser(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        devicenplatform_key = uaresolver::get_devicenbrowser_key(startpos, endpos);
    }
    
    void set_event_type(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        /* generated with PYTHON:
         for s in ("ping", "impr", "pgv2", "adreq", "adresp"):
          chars = [ord(c) for c in (s + "\0\0\0\0\0\0\0\0")][7::-1] + [s.upper()]
          print ("(n==0x%02x%02x%02x%02x%02x%02x%02x%02x) ? EventType::%s :" % tuple(chars))
         */
        uint64_t n = 0;
        strncpy((char*)&n, startpos, endpos-startpos);
        event_type =
                (n==0x00000000676e6970) ? EventType::PING :
                (n==0x0000000072706d69) ? EventType::IMPR :
                (n==0x0000000032766770) ? EventType::PGV2 :
                (n==0x0000007165726461) ? EventType::ADREQ :
                (n==0x0000707365726461) ? EventType::ADRESP :
                EventType::UNKNOWN;
    }
    
    void set_timestamp(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        timestamp = (uint32_t)(read_number(startpos, endpos) / 1000);
    }
    
    void set_timespent(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        timespent = (uint32_t)(read_number(startpos, endpos));
    }
    
    void set_user(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        scratch->user = startpos;
        scratch->user_len = endpos - startpos;
    }
    
    void set_client_type(
            char * const startpos,
            char * const endpos,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        client_type = (*startpos == 'd' ? 0 :
                       *startpos == 'p' ? 1 :
                       2);
    }
        
    void handle_keyvalue(
            char * const keystart,
            char * const keyend,
            char * const valstart,
            char * const valend,
            StringIndex * const stringindex,
            EventScratch * const scratch) {
        /**
         * Handles the supplied key and value in string *valstart to *valend
         */
        keyhandler_fn fn = key_map.get(keystart, keyend);
        if (fn != NULL) {
            (this->*fn)(valstart, valend, stringindex, scratch);
        }
    }
    
    void closeRecord(
            StringIndex *stringindex,
            EventScratch *scratch) {
        this->vic_user_sesn_ident = stringindex->set_compound(
            3,
            scratch->user, scratch->user_len,
            scratch->sesn, scratch->sesn_len,
            scratch->vic, scratch->vic_len);
    }
    
    static bool compare_by_ip(Event &a, Event &b) {
        /** a comparator to facilitate geoip lookups */
        return a.ip_value  < b.ip_value;
    }
    
    static bool compare_by_vic(Event &a, Event &b) {
        /** a comparator for getting events of the same vic info together */
        if (a.vic_user_sesn_ident < b.vic_user_sesn_ident) return true;
        if (a.vic_user_sesn_ident > b.vic_user_sesn_ident) return false;
        
        if (a.creative_key < b.creative_key) return true;
        if (a.creative_key > b.creative_key) return false;
        
        return a.event_id < b.event_id;
    }
};


#endif /* events_hpp */
