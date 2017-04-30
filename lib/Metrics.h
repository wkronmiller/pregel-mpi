#ifndef METRICS_H
#define METRICS_H

#ifdef __BGQ__
#include<hwi/include/bgqc/A2_inlines.h>
#else
#include<time.h>
#endif

// Custom metrics written by Rory/Adam for Parallel Computing final project
namespace Metrics {

namespace Counters {
    unsigned long long start_time, 
                        end_time, 
                        run_start_time,
                        run_end_time,
                        load_graph_time,
                        total_compute_time = 0,
                        store_vertices_time;
}

unsigned long long get_cycle_time() {
#ifdef __BGQ__
    return GetTimeBase();
#endif
    return clock();
}


};
#endif
