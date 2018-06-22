//
//  geoip.cpp
//  BigData
//
//  Created by Eric on 2015-12-27.
//  Copyright © 2015 eric. All rights reserved.
//

#include "geoip.hpp"
#include "memorymap.hpp"
#include "texthandling.hpp"
#include "defines.h"

namespace geoip {
        
    GeoIPDatabase::GeoIPDatabase() {
        places_by_hash.set_empty_key((placehash_t)0);
       
        MemoryMappedFile input_csv(GEO_IP_FILE);
        input_csv.grab();
        
        char buff[256];
        
        for (char *pos = (char*)input_csv.get_start();
             pos < (char*)input_csv.get_end();) {
            
            char *buffpos = buff;
            
            // advance into next quoted field
            pos = strchr(pos, '"') + 1;
            // read ip range
            uint32_t ip_start = read_ip(&pos);
            
            // advance into next quoted field
            pos = strchr(pos+1, '"') + 1;
            uint32_t ip_end = read_ip(&pos);
            
            // read geo: city, region, country
            char *city = read_str(
                &pos, &buffpos, sizeof(buff) - (buffpos - buff));
            
            pos = strchr(pos+1, '"') + 1;
            char *region = read_str(
                &pos, &buffpos, sizeof(buff) - (buffpos - buff));
            
            // advance into next quoted field
            pos = strchr(pos+1, '"') + 1;
            char *country = read_str(
                &pos, &buffpos, sizeof(buff) - (buffpos - buff));
            
            uint64_t place_hash = XXH64(buff, (buffpos - buff), 0);
            Place *place = places_by_hash[place_hash];
            if (place == nullptr) {
                // unpack 3 \0 delimited strings
                place = new Place(city, region, country);
                places_by_hash[place_hash] = place;
            }
            geoips.push_back(GeoIP(ip_start, ip_end, place));
            
            // next line
            pos = strchr(pos, '\n') + 1;
        }
        geoips.shrink_to_fit();
        
        input_csv.release();
    }
}