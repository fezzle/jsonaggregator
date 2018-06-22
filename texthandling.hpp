//
//  texthandling.hpp
//  QueryAgg
//
//  Created by Eric on 2017-01-23.
//  Copyright © 2017 norg. All rights reserved.
//

#ifndef texthandling_hpp
#define texthandling_hpp


#include <stdio.h>
#include "defines.h"

extern int hexlookup[];

/* Reads a 32-char hex-encoded field and returns the 16-byte integer */
uint128_t parse_16byte_hex(char *str);


/* Reads an IPv4 field from the char ptr *start and moves **start
 *  past the closing quote.
 */
uint32_t read_ip(char **start);


/* Reads a string field from the char ptr *input_buff and copies it to
 *  *output_buff incrementing both pointers until the quoted string is 
 *  read or buflen is reached.  Once completed, *input_buff points just past
 *  the close quote and *output_buff points just past the terminating '\0'.
 *
 * Returns pointer to start of output string (ie: initial value of *output_buff)
 */
char *read_str(char **input_buff, char **output_buff, unsigned long buflen);


/**
 Reads a base-10 number from *startpos to *endpos and returns the result.
 Stops reading when a non-digit character is found
 */
uint64_t read_number(char *itr, char *end);


#endif /* texthandling_hpp */
