//
//  ua_resolver.hpp
//  BigData
//
//  Created by Eric on 2016-01-17.
//  Copyright © 2016 eric. All rights reserved.
//

#ifndef ua_resolver_h
#define ua_resolver_h

#include <string>
#include <string.h>

#include "xxHash/xxhash.h"
#include "sparsehash/dense_hash_map"

#include "defines.h"

using namespace std;


namespace uaresolver {
    class DeviceNBrowser {
    public:
        const char *device;
        const char *browser;
        
        DeviceNBrowser() :
        device("unknown"),
        browser("unknown") {
            // pass
        }
        
        DeviceNBrowser(const char* device, const char* browser) :
        device(device),
        browser(browser) {
            // pass
        }
    };
    
    enum { SMARTPHONE=0, DESKTOP, END_DEVICES };
    inline int get_device(char *startpos, char *endpos) {
        size_t len = endpos-startpos;
        if (strnstr(startpos, "iPhone", len)) {
            return SMARTPHONE;
        }
        if (strnstr(startpos, "iPad", len)) {
            return SMARTPHONE;
        }
        if (strnstr(startpos, "android", len)) {
            return SMARTPHONE;
        }
        return DESKTOP;
    }
    
    enum { CHROME=0, MSIE, FIREFOX, SAFARI, UNKNOWN, END_BROWSERS };
    inline int get_browser(char *startpos, char *endpos) {
        size_t len = endpos-startpos;
        if (strnstr(startpos, "Chrome", len)) {
            return CHROME;
        }
        if (strnstr(startpos, "MSIE", len)) {
            return MSIE;
        }
        if (strnstr(startpos, "Firefox", len)) {
            return FIREFOX;
        }
        if (strnstr(startpos, "Safari", len)) {
            return SAFARI;
        }
        return UNKNOWN;
    }
    
    static DeviceNBrowser table[END_DEVICES][END_BROWSERS];
    inline void static_init() {
        table[SMARTPHONE][CHROME] = DeviceNBrowser("smartphone", "chrome");
        table[SMARTPHONE][MSIE] = DeviceNBrowser("smartphone", "msie");
        table[SMARTPHONE][FIREFOX] = DeviceNBrowser("smartphone", "firefox");
        table[SMARTPHONE][SAFARI] = DeviceNBrowser("smartphone", "safari");
        table[SMARTPHONE][UNKNOWN] = DeviceNBrowser("smartphone", "unknown");
        table[DESKTOP][CHROME] = DeviceNBrowser("desktop", "chrome");
        table[DESKTOP][MSIE] = DeviceNBrowser("desktop", "msie");
        table[DESKTOP][FIREFOX] = DeviceNBrowser("desktop", "firefox");
        table[DESKTOP][SAFARI] = DeviceNBrowser("desktop", "safari");
        table[DESKTOP][UNKNOWN] = DeviceNBrowser("desktop", "unknown");
    }
    
    inline small_key get_devicenbrowser_key(char *startpos, char *endpos) {
        return ((get_device(startpos, endpos) * END_BROWSERS) +
            get_browser(startpos, endpos));
    }
    
    inline DeviceNBrowser *get_devicenbrowser_fromkey(small_key key) {
        return &table[key / END_BROWSERS][key % END_BROWSERS];
    }
}

#endif /* ua_resolver_h */
