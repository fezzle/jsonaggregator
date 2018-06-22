//
//  timer.hpp
//  BigData
//
//  Created by Eric on 2016-08-06.
//  Copyright © 2016 eric. All rights reserved.
//

#ifndef timer_hpp
#define timer_hpp

#include <stdio.h>
#include <boost/log/trivial.hpp>


class Timer {
public:
    clock_t t0;
    char buff[256];
    
    Timer(const char *msg, ...) {
        t0 = clock();
        
        va_list args;
        va_start (args, msg);
        vsnprintf(buff, sizeof(buff), msg, args);
        va_end (args);
    }
    
    ~Timer() {
        clock_t t1 = clock();
        float t_delta = ((float)(t1-t0))/CLOCKS_PER_SEC;
        BOOST_LOG_TRIVIAL(info) << buff << " took " << t_delta << " seconds ";
    }
};

#endif /* timer_hpp */
