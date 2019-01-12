//
//  executor.cpp
//  QueryAgg
//
//  Created by Eric on 2017-01-22.
//  Copyright © 2017 norg. All rights reserved.
//

#include "workflow.hpp"

#include <algorithm>
#include <chrono>
#include <forward_list>
#include <string>
#include <thread>
#include <vector>
#include <iostream>
#include <assert.h>
#include <utility>

#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <tbb/parallel_invoke.h>
#include <tbb/parallel_sort.h>
#include <tbb/scalable_allocator.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/task_group.h>
#include <tbb/task.h>
#include <tbb/combinable.h>


#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>


#include "xxHash/xxhash.h"
#include "defines.h"

#include "memorymanager.hpp"
#include "timer.hpp"
#include "geoip.hpp"
#include "ua_resolver.hpp"
#include "events.hpp"
#include "stringindex.hpp"
#include "jsonparser.hpp"
#include "session.hpp"
#include "aggevents.hpp"

#define ESTIMATE_RECORDSIZE_BYTES_PER_FILE_TO_CHECK 1<<24
#define MAX_EXPECTED_RECORD_SIZE 1<<20


using namespace std;
using namespace boost::filesystem;

using namespace uaresolver;
using namespace geoip;

void static_inits() {
    /**
     Initializes singletons and calls known class::static_init methods 
     */
    Timer t1("Static Inits");
    Event::static_init();
    GeoIPDatabase::get_instance();
    uaresolver::static_init();
    
    assert(sizeof(Session) == 64);
    assert(sizeof(Event) == 64);
}


size_t compute_total_filesize(const vector<string> &inputfiles) {
    Timer t1("Summing all file sizes");
    size_t total_size = 0;
    for (auto itr=inputfiles.begin(); itr != inputfiles.end(); itr++) {
        total_size += file_size(*itr);
    }
    return total_size;
}


size_t estimate_recordsize(
        const vector<string> &inputfiles,
        char record_separator) {
    Timer t1("Estimating bytes per record");
    
    return 800;
}


void check_useragent_parsing(
        const vector<string> &inputfiles,
        char record_separator) {
    Timer t1("Checking useragent parsing");
    
    char *useragent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36";
    
    small_key devicenplat = get_devicenbrowser_key(
        useragent, useragent + strlen(useragent));
    
    
    DeviceNBrowser *dp = get_devicenbrowser_fromkey(devicenplat);
    
    BOOST_LOG_TRIVIAL(info)
        << "found browser:" << dp->browser
        << " device:" << dp->device << endl;
}

StringIndex *merge_stringindexes() {
    Timer t1("Merging String Indexes");
    StringIndex *stringindex = StringIndex::combine_all();
    return stringindex;
}


template<typename JSONCLASS, typename SCRATCHCLASS>
void parse_json(size_t thread_count,
                MemoryManager<JSONCLASS> *memorymanager,
                const vector<string> &inputfiles) {
    /**
     Parses the supplied json using thread_count workers.
     
     WorkManager is used to break up the effort.
     
     MemoryManager is used to request chunks of memory.
     
     Uses the events/JsonParser<Event> task.
    */
    Timer t1("Parsing json files");
    
    FileSetStream stream(inputfiles, '\n');
    
    FileSetStreamIterator *inputstream = stream.get_itr();
    MemoryManagerIterator<JSONCLASS> *outputstream = memorymanager->get_itr();
    
    tbb::parallel_for(tbb::blocked_range<size_t>(0, thread_count, 1),
        [=](const tbb::blocked_range<size_t>& r) {
            JsonParser<JSONCLASS, SCRATCHCLASS> parser(
                StringIndex::new_instance(), inputstream, outputstream);
            parser.go();
        }
    );
}


void sort_events_by_ip(MemoryManager<Event> *memorymanager) {
    Timer t1("Sorting Events by ip");
    tbb::parallel_sort(memorymanager->get_start(),
                       memorymanager->get_end(),
                       Event::compare_by_ip);
}



void sort_events_by_session(MemoryManager<Event> *memorymanager) {
    Timer t1("Sorting Events by session key");
    size_t event_count = memorymanager->get_end() - memorymanager->get_start();
    tbb::parallel_sort(memorymanager->get_start(),
                       memorymanager->get_start() + event_count,
                       Event::compare_event_by_vic);
}


void verify_sorted(MemoryManager<Event> *memorymanager) {
    Timer t1("Verifying sorted");
    Event *events = memorymanager->get_start();
    size_t event_count = memorymanager->get_end() - memorymanager->get_start();
    BOOST_LOG_TRIVIAL(info) << "Event Count: " << event_count << endl;
    
    for (size_t i=1; i<event_count; i++) {
        BOOST_ASSERT_MSG(
            events[i].vic_user_sesn_ident >= events[i-1].vic_user_sesn_ident,
            "Events were not sorted correctly");
    }
}

    
void groupby_session(
        size_t thread_count,
        MemoryManagerIterator<Event> *inputstream,
        MemoryManagerIterator<Session> *outputstream) {

    tbb::parallel_for(tbb::blocked_range<size_t>(0, thread_count, 1),
        [=](const tbb::blocked_range<size_t>& r) {
            Event *event_pos = nullptr;
            Event *event_end = nullptr;
            
            Session *session_pos = nullptr;
            Session *session_end = nullptr;
            
            while (true) {
                if (event_pos >= event_end) {
                    pair<Event*, Event*> range = inputstream->get_next();
                    
                    if (range.first != nullptr) {
                        Event *start_of_input = inputstream->get_start();
                        event_pos = range.first;
                        event_end = range.second;
                        // rewind to start of this vic-sesn-user if not first record
                        while (event_pos > start_of_input &&
                                event_pos[0].vic_user_sesn_ident ==
                                event_pos[-1].vic_user_sesn_ident) {
                            event_pos--;
                        }
                    } else {
                        // end of input stream
                        bzero(session_pos,
                            (size_t)session_end - (size_t)session_pos);
                        break;
                    }
                }
                
                if (session_pos >= session_end) {
                    pair<Session*, Session*> range = outputstream->get_next();
                    if (range.first != nullptr) {
                        session_pos = range.first;
                        session_end = range.second;
                    } else {
                        BOOST_ASSERT_MSG(false, "OOM for Session data");
                    }
                }
                
                //bool is_last = event_end == inputstream->get_end();
                while (event_pos < event_end && session_pos < session_end) {
                    Session *session_acc =
                        new (session_pos) Session(event_pos);
                    session_pos++;
                    size_t last_event_id = -1;
                    uint32_t first_timestamp = event_pos->timestamp;
                    
                    while (event_pos < event_end &&
                            event_pos->vic_user_sesn_ident ==
                                session_acc->vic_user_sesn_ident) {
                        // dedupe events
                        if (event_pos->event_id == last_event_id) {
                            event_pos++;
                            continue;
                        } else {
                            last_event_id = event_pos->event_id;
                        }
                        
                        switch(event_pos->event_type) {
                        case PING:
                            session_acc->pings++;
                            uint32_t tdelta = event_pos->timestamp - first_timestamp;
                            if (tdelta > session_acc->timespent) {
                                session_acc->timespent = tdelta;
                            }
                            break;
                        case IMPR:
                            session_acc->impressions++;
                            break;
                        case PGV2:
                            session_acc->views++;
                            break;
                        case ADREQ:
                            session_acc->adrequests++;
                            break;
                        case ADRESP:
                            session_acc->adresponses++;
                            break;
                        }
                        event_pos++;
                    }
                }
            }
        }
    );
}


void sort_sessions_by_ip(MemoryManager<Session> *memorymanager) {
    Timer t1("Sorting Sessions by IP");
    size_t event_count = memorymanager->get_end() - memorymanager->get_start();
    tbb::parallel_sort(memorymanager->get_start(),
                       memorymanager->get_start() + event_count,
                       Session::compare_session_by_ip_value);
}


void map_geoip_lookup(
        size_t thread_count, MemoryManager<Session> *memorymanager) {
    
    auto *session_itr = memorymanager->get_itr();
    GeoIPDatabase *geodb = GeoIPDatabase::get_instance();
    
    tbb::parallel_for(tbb::blocked_range<size_t>(0, thread_count, 1),
        [=](const tbb::blocked_range<size_t>& r) {
            Session *session_pos = nullptr;
            Session *session_end = nullptr;
            
            while (true) {
                if (session_pos >= session_end) {
                    pair<Session*, Session*> range = session_itr->get_next();
                    if (range.first != nullptr) {
                        session_pos = range.first;
                        session_end = range.second;
                    } else {
                        return;
                    }
                }
                
                int last_ip_pos = 0;
                while (session_pos >= session_end) {
                    session_pos->geoip_place = geodb->get_ip(
                        session_pos->ip_value, &last_ip_pos);
                    session_pos++;
                }
            }
        }
    );
}


void groupby_aggregate(
        size_t thread_count,
        MemoryManagerIterator<Session> *inputstream,
        MemoryManagerIterator<AggEvents> *outputstream) {
    
}


void run_workflow(int thread_count, vector<string> &inputfiles) {
    static_inits();

    check_useragent_parsing(inputfiles, thread_count);

    size_t total_size = compute_total_filesize(inputfiles);
    size_t estimated_record_size = estimate_recordsize(inputfiles, '\n');
    
    size_t estimated_record_count = (total_size / estimated_record_size);
    // add 10% for buffer
    estimated_record_count += estimated_record_count / 10;
    
    MemoryManager<Event> event_memorymanager(estimated_record_count, 1<<20);
    parse_json<Event, EventScratch>(thread_count, &event_memorymanager, inputfiles);
 
    sort_events_by_session(&event_memorymanager);
    
    StringIndex *master_string_index = merge_stringindexes();
    
    BOOST_LOG_TRIVIAL(info)
        << "StringIndex Count:" << master_string_index->stringmap.size()
        << "  String Bytes:" << master_string_index->strings.size()
        << endl;
    
    verify_sorted(&event_memorymanager);
    
    MemoryManager<Session> session_memorymanager(
        estimated_record_count/4, 1<<16);
    
    groupby_session(
        thread_count,
        event_memorymanager.get_itr(),
        session_memorymanager.get_itr());
    
    event_memorymanager.clear();
    
    sort_sessions_by_ip(&session_memorymanager);
    
    map_geoip_lookup(thread_count, &session_memorymanager);
    
    MemoryManager<Aggregate> aggregate_memorymanager(
        estimated_record_count/20, 1<<20);
    
    groupby_aggregate(
        thread_count,
        session_memorymanager.get_itr(),
        aggregate_memorymanager.get_itr());
    
    
        
}