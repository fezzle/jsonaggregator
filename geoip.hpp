//
//  geoip.hpp
//  BigData
//
//  Created by Eric on 2015-12-27.
//  Copyright © 2015 eric. All rights reserved.
//

#ifndef geoip_hpp
#define geoip_hpp

#include <array>
#include <string>
#include <vector>

#include <fcntl.h>
#include <stdio.h>
#include <inttypes.h>
#include <sys/errno.h>

#include "xxhash.h"
#include "sparsehash/dense_hash_map"

#include "texthandling.hpp"
#include "defines.h"

#define MAX_REGION_LEN 65
#define MAX_CITY_LEN 67
#define MAX_COUNTRY_LEN 2

using namespace std;

namespace geoip {
    using google::dense_hash_map;

    typedef unsigned long long placehash_t;
        
    class Place {
    public:
        std::string city;
        std::string region;
        std::string country;
        
        Place(char *city, char *region, char *country) :
                city(city),
                region(region),
                country(country) {
        }
    };
    
    
    class GeoIP {
    public:
        uint32_t ip_start;
        uint32_t ip_end;
        Place *place;
        
        GeoIP(uint32_t ip_start, uint32_t ip_end, Place *place):
            ip_start(ip_start),
            ip_end(ip_end),
            place(place) {
            // pass
        }
    };
    
    
    class GeoIPDatabase {
    private:
        dense_hash_map<placehash_t, Place*> places_by_hash;
        vector<GeoIP> geoips;
        GeoIPDatabase();
        
        GeoIPDatabase(GeoIPDatabase const&);
        void operator=(GeoIPDatabase const&);
    public:
        static GeoIPDatabase* get_instance() {
            static GeoIPDatabase *instance;
            if (instance == nullptr) {
                instance = new GeoIPDatabase();
            }
            return instance;
        }
        
        vector<GeoIP> *get_geo_ips() {
            return &geoips;
        }
        
        Place *get_ip(size_t ip_value, int *last_index) {
            int pos = *last_index;
            
            // linear search 4 IP values before doing binary search
            for (int attempts=4; attempts>0; attempts--) {
                if (geoips[pos].ip_start >= ip_value &&
                        geoips[pos].ip_end <= ip_value) {
                    *last_index = pos;
                    return geoips[pos].place;
                } else if (geoips[pos].ip_start > ip_value && pos>0) {
                    pos--;
                    if (geoips[pos].ip_end < ip_value) {
                        return nullptr;
                    }
                } else if (geoips[pos].ip_end < ip_value && pos<geoips.size()-1) {
                    pos++;
                    if (geoips[pos].ip_start > ip_value) {
                        return nullptr;
                    }
                }
            }
            
            // binary search
            unsigned long first = 0;
            unsigned long last = geoips.size()-1;
            while (first <= last) {
                int middle = (first + last) / 2;
                if (geoips[middle].ip_start >= ip_value &&
                        geoips[middle].ip_end <= ip_value) {
                    *last_index = middle;
                    return geoips[middle].place;
                } else if (geoips[middle].ip_end < ip_value) {
                    first = middle + 1;
                } else {
                    last = middle - 1;
                }
            }
            return nullptr;
        }
    };
}
            
#endif /* geoip_hpp */
