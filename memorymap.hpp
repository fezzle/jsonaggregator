//
//  memorymap.hpp
//  QueryAgg
//
//  Created by Eric on 2017-01-23.
//  Copyright © 2017 norg. All rights reserved.
//

#ifndef memorymap_hpp
#define memorymap_hpp

#include <algorithm>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <iterator>

#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/errno.h>

#include <boost/log/trivial.hpp>

#include <tbb/mutex.h>

#include "defines.h"


using namespace std;


class MemoryMappedFile : input_iterator_tag {
private:
    // disallow copies, default ctors, moves
    MemoryMappedFile(MemoryMappedFile const&) = delete;
    void operator=(MemoryMappedFile const&) = delete;
    MemoryMappedFile() = delete;
    MemoryMappedFile(MemoryMappedFile&&) = delete;
    MemoryMappedFile& operator=(MemoryMappedFile&&) = delete;
    
    struct stat sb;
    char *data;
    
public:
    uint64_t users;
    size_t size;
    string filename;
    
    MemoryMappedFile(string filename):
            filename(filename),
            data(nullptr),
            users(0) {
        if (stat(filename.c_str(), &sb) == -1) {
            throw std::invalid_argument(strerror(errno));
        }
    }
    
    void grab() {
        if (users++ == 0) {
            open();
        }
    }
    
    void release() {
        if (--users == 0) {
            close();
        }
    }
    
    size_t get_size() {
        return size;
    }
    
    char *get_start() {
        return data;
    }
    
    char *get_end() {
        return (char*)((size_t)data + sb.st_size);
    }
    
    void open() {
        BOOST_LOG_TRIVIAL(info) << "Opening memorymap: " << filename << endl;
        
        int fd = ::open(filename.c_str(), O_RDONLY);
        if (fd == -1) {
            throw std::invalid_argument(strerror(errno));
        }
        data = (char *)mmap(0, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        
        if (data == MAP_FAILED) {
            throw std::invalid_argument(strerror(errno));
        }
        
        madvise(data, sb.st_size, MADV_SEQUENTIAL|MADV_WILLNEED);
  
        size = sb.st_size;
        
        ::close(fd);
    }
    
    void close() {
        BOOST_LOG_TRIVIAL(info) << "Closing memorymap: " << filename << endl;
        munmap(data, sb.st_size);
    }
};


#endif /* memorymap_hpp */
