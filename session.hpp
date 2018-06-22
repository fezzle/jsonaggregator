//
//  session.hpp
//  QueryAgg
//
//  Created by Eric on 2017-09-25.
//  Copyright © 2017 norg. All rights reserved.
//

#ifndef session_hpp
#define session_hpp

#include <stdio.h>
#include <stdint.h>

#include "defines.h"
#include "texthandling.hpp"
#include "stringindex.hpp"
#include "geoip.hpp"
#include "events.hpp"


class Session {
public:
    /* 32 bytes */
    hash_key property_key;
    hash_key creative_key;
    hash_key placementlabel_key;
    hash_key vic_user_sesn_ident;
    
    /* 4 bytes */
    small_key devicenplatform_key;
    
    /* 8 bytes */
    union {
        uint32_t ip_value;
        geoip::Place *geoip_place;
    };
    
    /* 6*2 + 4 = 16 bytes */
    float timespent;
    uint16_t pings;
    uint16_t views;
    uint16_t impressions;
    uint16_t adrequests;
    uint16_t adresponses;
    
    Session() {}

    Session(Event *event):
        property_key(event->property_key),
        creative_key(event->creative_key),
        placementlabel_key(event->placementlabel_key),
        devicenplatform_key(event->devicenplatform_key),
        vic_user_sesn_ident(event->vic_user_sesn_ident),
        ip_value(event->ip_value),
        views(0),
        impressions(0),
        adrequests(0),
        adresponses(0),
        timespent(0) {
        //pass
    }
    
    static bool compare_session_by_ip_value(Session &a, Session &b) {
        return a.ip_value > b.ip_value;
    }
};



#endif /* session_hpp */
