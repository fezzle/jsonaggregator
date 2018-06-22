//
//  main.cpp
//  QueryAgg
//
//  Created by Eric on 2017-01-21.
//  Copyright © 2017 norg. All rights reserved.
//

#include <iostream>
#include <stdlib.h>

#include <algorithm>
#include <chrono>
#include <forward_list>
#include <string>
#include <thread>
#include <vector>
#include <iostream>


#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <tbb/parallel_invoke.h>
#include <tbb/parallel_sort.h>
#include <tbb/scalable_allocator.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/task_group.h>


#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>

#include "xxhash/xxhash.h"
#include "flat_hash_map.hpp"
#include "workflow.hpp"

using namespace std;
using namespace boost::program_options;
using namespace boost::filesystem;



int main(int argc, const char * argv[]) {
    boost::log::add_console_log(std::cout, boost::log::keywords::format = ">> %Message%");

    const size_t thread_count = tbb::task_scheduler_init::default_num_threads();
    
    options_description
        desc{"Aggregate JSON encoded Pixel Events"};
    
    desc.add_options()
            ("help,h", "Help")
            ("path,p", value<vector<string>>(), "Directory or json file path")
            ("threads,t",
                value<size_t>()->default_value(thread_count),
                "Thread count (default:4)")
            ("record_count,c",
                value<size_t>()->default_value(0),
                "Record count (0 to guess)");
    
    variables_map vm;
    store(parse_command_line(argc, argv, desc), vm);
    
    vector<string> input_files;

    if (vm.count("path")) {
        vector<string> input_paths = vm["path"].as<vector<string>>();

        for (auto path_itr=input_paths.begin();
                path_itr != input_paths.end();
                path_itr++) {
            
            if (is_directory(*path_itr)) {
                for (recursive_directory_iterator dir(*path_itr), end;
                        dir != end;
                        dir++) {
                    input_files.push_back((*dir).path().string());
                }
            } else if (exists(*path_itr)) {
                input_files.push_back(*path_itr);
            } else {
                BOOST_LOG_TRIVIAL(error)
                    << "Input path does not exist: " << *path_itr << endl;
            }
        }
    }

    string tohash = "asdf";
    BOOST_LOG_TRIVIAL(info) << "Threads:" << thread_count << endl
                            << "Files:" << input_files.size() << endl
                            << "Hash of str: " << XXH64(tohash.c_str(), tohash.length(), 0);
    
    run_workflow(input_files, thread_count);
    
    return 0;

}
