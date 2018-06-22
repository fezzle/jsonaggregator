//
//  texthandling.cpp
//  QueryAgg
//
//  Created by Eric on 2017-01-23.
//  Copyright © 2017 norg. All rights reserved.
//

#include "texthandling.hpp"

#include <inttypes.h>
#include <string.h>
#include "texthandling.hpp"
#include "defines.h"

int hexlookup[] = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 10, 10, 10,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 10, 11, 12, 13, 14, 15 };


uint128_t parse_16byte_hex(char *str) {
    uint128_t res;
    uint64_t acc = 0;
    acc = hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    res = acc;
    res <<= 64;
    
    acc = hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    acc = (acc << 4) + hexlookup[(*str++) - '0'];
    res += acc;
    
    return res;
}


uint32_t read_ip(char **start) {
    /* Reads dotted IP from input until first non-digit started octet is found.
     * Updates *start to position of terminating quote.
     */
    char *pos = *start;
    
    uint32_t octet = 0;
    while (*pos >= '0' && *pos <= '9') {
        uint32_t byteacc = 0;
        while (1) {
            char d = *pos;
            if (d >= '0' && d <= '9') {
                byteacc = byteacc * 10 + (d - '0');
            } else {
                octet = (octet << 8) | byteacc;
                if (*pos == '.') {
                    pos++;
                }
                break;
            }
            pos++;
        }
    }
    *start = pos;
    // return with *start one past quote(*")
    return octet;
}


uint64_t read_number(char *itr, char *end) {
    uint64_t v = 0;
    while (itr < end && *itr >= '0' && *itr <= '9') {
        v = (v*10) + (*itr-'0');
        itr++;
    }
    return v;
}


char *read_str(char **input_buff, char **output_buff, unsigned long bufflen) {
    /* Reads string into output buff until bufflen is reached or terminating
     * quote is encountered.  On return, *input_buff points to terminating quote,
     * and *output_buff points to character after terminating null.
     */
    char *inpos = *input_buff;
    char *outpos = *output_buff;
    
    while (bufflen-->0) {
        if (inpos[0] == '"' && inpos[-1] != '\\') {
            break;
        }
        *outpos++ = *inpos++;
    }
    if (bufflen == 0) {
        outpos--;
    }
    *outpos++ = '\0';
    
    char *start = *output_buff;
    
    // increment output buffer
    *output_buff = outpos;
    
    // leave input pointing to terminating quote
    *input_buff = inpos;
    
    return start;
}
