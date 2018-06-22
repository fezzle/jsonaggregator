//
//  aggevents.hpp
//  QueryAgg
//
//  Created by Eric on 2017-09-26.
//  Copyright © 2017 norg. All rights reserved.
//

#ifndef aggevents_hpp
#define aggevents_hpp

#include <stdio.h>

#include "defines.h"
#include "session.hpp"
#include "geoip.hpp"

class Aggregate {
public:
    /* 32 bytes */
    hash_key property_key;
    hash_key creative_key;
    hash_key placementlabel_key;
    
    /* 4 bytes */
    small_key devicenplatform_key;
    geoip::Place *geoip_place;
    
    /* 6*2 + 4 = 16 bytes */
    float timespent;
    uint16_t pings;
    uint16_t views;
    uint16_t impressions;
    uint16_t adrequests;
    uint16_t adresponses;
    
    Aggregate() {}

    Aggregate(Session *session):
        property_key(session->property_key),
        creative_key(session->creative_key),
        placementlabel_key(session->placementlabel_key),
        devicenplatform_key(session->devicenplatform_key),
        geoip_place(session->geoip_place),
        views(0),
        impressions(0),
        adrequests(0),
        adresponses(0),
        timespent(0) {
        //pass
    }
};



#endif /* aggevents_hpp */
