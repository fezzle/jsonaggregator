#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <utility>
#include <unistd.h>
#include <fcntl.h>

#include <tbb/tbb.h>


#define CHUNK_SIZE (1<<26)

#include "xxHash/xxhash.h"

using namespace std;
using namespace tbb::flow;

class InputMemoryMapChunk {
    void *memorymap;
    void *start;
    void *end;
    InputMemoryMapChunk(void *memorymap, void *start, void *end) : 
            memorymap(memorymap), start(start), end(end) {
        // nothing
    }
};

class PixelRecord {
    uint16_t session;
    
};


int main(int argc, char **argv) {
    graph g;

    // source node for input files
    source_node<string> input_files(g, [argc, argv](string &inputfile) -> int {
        if (--argc) {
            inputfile.append(argv[argc]);
            return true;
        } else {
            return false;
        }
    }, true);

    typedef multifunction_node<string, tuple<InputMemoryMapChunk> > file_mmap_chunker;
    file_mmap_chunker memory_map_splitter(
            g, unlimited, [] -> (string inputfile, file_mmap_chunker::output_ports_type &op) {
        struct stat sb;            
        size_t size;                
        if (stat(inputfile.c_str(), &sb) == -1) {
            throw std::invalid_argument(strerror(errno));
        }
        int fd = open(inputfile.c_str(), O_RDONLY);
        if (fd == -1) {
            throw std::invalid_argument(strerror(errno));
        }
        char *mmap_ptr = (char *)mmap(0, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mmap_ptr == MAP_FAILED) {
            throw std::invalid_argument(strerror(errno));
        }
        char *mmap_end_ptr = mmap_ptr + sb.st_size;
        madvise(mmap_ptr, sb.st_size, MADV_SEQUENTIAL|MADV_WILLNEED);        
        for (char *startitr=mmap_ptr; startitr < mmap_end_ptr;) {
            char *enditr = startitr + BLOCKSIZE;
            if (startitr != mmap_ptr) {
                // find next newline
                while (*enditr != '\n' && enditr < mmap_end_ptr) { 
                    enditr++;
                };
            }
            std::get<0>(op).try_put(InputMemoryMapChunk(mmap_ptr, startitr, enditr));
            startitr = enditr + 1;
        }
    });

    function_node<InputMemoryMapChunk, PixelRecord> f1( g, 1, []( int i ) -> int {
        
    cout << "f1 consuming " << i << "\n";
    return i; 
    } );
    
      function_node< int, int, rejecting > f2( g, 1, []( int i ) -> int {
        sleep(2);
        cout << "f2 consuming " << i << "\n";
        return i; 
      } );
    
      priority_queue_node< int > q(g);
    
      make_edge( q, f1 );
      make_edge( q, f2 );
      for ( int i = 10; i > 0; --i ) {
        q.try_put( i );
      }
      g.wait_for_all();

    return 0;
}