#ifndef workflow_hpp
#define workflow_hpp

#include <stdio.h>
#include <stdint.h>
#include <algorithm>
#include <utility>
#include <string>
#include <vector>

#include <tbb/scalable_allocator.h>

using namespace std;

void run_workflow(vector<string> &inputfiles, int threadcount);


#endif /* workflow_hpp */
