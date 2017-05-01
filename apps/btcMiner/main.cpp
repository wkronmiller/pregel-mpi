#include"Pregel.h"
#include<iostream>
#include<fstream>
#include<vector>
#include<algorithm>
#include<limits.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>

#define DUMMY_GRAPH 1
#define DEBUG_NUM_EDGES 20

#define MAX_GRAPH_NODES 1000000 
#define MAX_EDGE_DEGREE MAX_GRAPH_NODES - 1
#define DEFAULT_VERTEX_VALUE -1 // Default vertex value is -1 (not in walk)

#define MIN(a,b) (((a)<(b))?(a):(b))

// Default checked function error-handler
void handleError(int err, int myrank) {
    // TODO: make myrank global
    if (err) {
        char estr[256] = {0};
        int len = 0;
        MPI_Error_string(err, estr, &len);
        fprintf(stderr,"[%u]: MPI error: %s\n", myrank, estr);
        fflush(stderr);
        sleep(10); // Sleep to give MPI time to log message
        MPI_Abort(MPI_COMM_WORLD, err);
    }
}

// Int is walk id or -1
typedef int VertexValue;

// Double is edge weight
typedef double VertexEdge;

// Int is walk id
typedef int BTCMessage;

class BTCCombiner:public Pregel::BaseCombiner<BTCMessage> {
public:
    inline void combine(const BTCMessage& old_message, const BTCMessage& new_message) {
        //No-op (vertex joins only one walk)
    }
};

namespace BTCSettings {
    unsigned int max_iterations;
    unsigned int branching_factor = 1; // Number of children to invite to a walk
    double p;
};

double extract_weight(Pregel::BaseEdge<VertexEdge> edge) {
    return edge.value;
}

class BTCVertex:public Pregel::BaseVertex<VertexValue, VertexEdge, BTCMessage, Pregel::DefaultHash, BTCCombiner> {
private:
    typedef Pregel::BaseVertex<VertexValue, VertexEdge, BTCMessage, Pregel::DefaultHash, BTCCombiner> BaseVertexType;

    // Return randomly selected index from vector of weights
    // https://softwareengineering.stackexchange.com/questions/150616/return-random-list-item-by-its-weight
    unsigned int select_weighted_index(std::vector<double>& weights) {
        const double random_number = ((double) rand()) / RAND_MAX;
        double cumulative_sum = 0;
        for(unsigned int index = 0; index < weights.size(); ++index) {
            cumulative_sum += weights[index];
            if(random_number < cumulative_sum) {
                return index;
            }
        }
        //TODO: throw exception
        std::cerr << "Failed to select index correctly (cumsum = " << cumulative_sum;
        std::cerr << ", number of weights = " << weights.size() << ", ";
        std::cerr << "number of edges = " << edges().size() << ", ";
        std::cerr << "id " << id() << ")" << std::endl;
        return 0;
    }

#if 0
    void disable_edge(const unsigned int chosen_index, std::vector<double>& weights) {
        double edge_weight = edges[chosen_index];
        edges[chosen_index] = 0;
        //TODO: divide by zero check
        double push_weight = edge_weight / (edges.size() - 1);

    }
#endif

    // Invite a child to a walk
    void send_to_children(VertexValue& value) {
        std::vector<double> weights;
        weights.resize(edges().size());
        std::transform(edges().begin(), edges().end(), weights.begin(), extract_weight);
        assert(weights.size() == edges().size());
        for(unsigned int invite_num = 0; invite_num < BTCSettings::branching_factor && invite_num < weights.size(); ++invite_num) {
            unsigned int chosen_index = select_weighted_index(weights);
            //TODO: zero out child and renormalize remaining weights to avoid sending to same child twice
            Pregel::VertexID target = edges().at(chosen_index).target;
            send_message(target, value);
        }
    }
    // Decide whether to initiate a walk
    bool start_walk() {
        if(step_num() >= (int)BTCSettings::max_iterations) {
            return true;
        }
        const double prob_start = BTCSettings::p * (((double)step_num()) / ((double)BTCSettings::max_iterations));
        return (double) rand() / RAND_MAX < prob_start;
    }
public:
    BTCVertex():BaseVertexType(){};
    BTCVertex(Pregel::VertexID id):BaseVertexType(id){};
    BTCVertex(Pregel::VertexID id, VertexValue value):BaseVertexType(id, value){};

    //TODO: check for a message, pick first message, join that thing's group if there's a message
    //If no message, randomly decide (see formula in main) whether to start walk
    //If now in a walk, randomly pick a child to send to based on edge weights
    //Once part of a group and after sending message to child, call vote_for_halt()
    void compute(const MessageContainer& messages) {
        if(value() != DEFAULT_VERTEX_VALUE) {
            Pregel::vote_for_halt();
            return;
        }
        unsigned long long compute_end, compute_start = Metrics::get_cycle_time();
        // Choose message if message, otherwise randomly decide to start walk using vertex id
        if(messages.empty() == false) {
            value() = messages.front();
        } else if (start_walk()) {
            value() = id();
        }
        compute_end = Metrics::get_cycle_time();
        Metrics::Counters::total_compute_time += (compute_end - compute_start);
        if(value() != DEFAULT_VERTEX_VALUE) {
            //TODO: logging
            send_to_children(value());
            Pregel::vote_for_halt();
        }
    }
};


class BTCGraphLoader:public Pregel::BaseGraphLoader<BTCVertex>{
private:
    int myrank;
    int commsize;

    void load_graph_mpi(const std::string& input_file) {
        const int myrank = Pregel::get_worker_id();
        const int commsize = Pregel::get_num_workers();

        // Stolen from btc-graph-miner sorta
        MPI_Offset file_size;
        MPI_File fh;
        int err;
        char infile[256] = {0};

        std::copy(input_file.begin(), input_file.end(), infile);
        err = MPI_File_open(MPI_COMM_WORLD, infile, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
        handleError(err, myrank);

        err = MPI_File_get_size(fh, &file_size);
        handleError(err, myrank);

        const int rank_chunk_size = (file_size / commsize);
        if (rank_chunk_size >= INT_MAX) {
            printf("Rank chunk too large, use more ranks");
            abort();
        }
        const MPI_Offset start_offset = rank_chunk_size * myrank;
        MPI_Offset inner_end_offset = start_offset + rank_chunk_size;

        if(file_size - inner_end_offset < rank_chunk_size) {
            inner_end_offset = file_size;
        }
        const int outer_buf_max = MIN(32768, file_size - inner_end_offset);
        char* buffer = (char*)calloc(sizeof(char), (rank_chunk_size + 2 + outer_buf_max));

        // We don't do anything with this. Should be fine however
        MPI_Status read_status;
        err = MPI_File_set_view(fh, start_offset, MPI_CHAR, MPI_CHAR, "native", MPI_INFO_NULL);
        handleError(err, myrank);

        // Don't think there's much risk of this not getting everything...
        // Either loop or just get a bunch of useless stuff. This is simpler :p
        err = MPI_File_read(fh, buffer, rank_chunk_size, MPI_CHAR, &read_status);
        handleError(err, myrank);

        // Find where the next newline is.
        char* end_buffer_pointer = buffer + rank_chunk_size;

        err = MPI_File_read(fh, end_buffer_pointer, outer_buf_max, MPI_CHAR, &read_status);
        handleError(err, myrank);

        char* tp = end_buffer_pointer;
        while (*tp != '\n' && *tp != '\0') {
            tp++;
        }
        *tp = '\0';
        *(tp+1) = '\0';
        *(end_buffer_pointer + outer_buf_max) = '\0';

        tp = buffer;

        // may keep things from breaking but also loose some data. Too
        // tired to think about edge cases
        while (*tp != '\n') tp++;

        char numbuf[64] = {0};;
        char* tnumbuf = numbuf;
        int i = 0;
        while (*tp != '\0') {
            // Grab the node id
            while (*tp != ';') {
                *(tnumbuf++) = *(tp++);
                if (*tp == '\0') break;
            }
            if (*tp == '\0') break;
            *tnumbuf = '\0';
            tp++;
            tnumbuf = numbuf;
            long long int nodeid = atoll(numbuf);
            add_vertex(nodeid, DEFAULT_VERTEX_VALUE);

            while (*tp != '\n' && *tp != '\0') {
                char start = *tp;
                while (*tp != ':') {
                    *(tnumbuf++) = *(tp++);
                    // Can probably remove this
                    if ((tnumbuf - numbuf) > 60){
                        printf("Start: 0x%02x Long numbuf: %.100s\n", start, tp - 100);
                        break;
                    }
                    if (*tp == '\0') break;
                }
                if (*tp == '\0') break;
                *tnumbuf = '\0';
                int dest_nodeid = atoi(numbuf);
                tnumbuf = numbuf;
                memset(numbuf, 0, 64);

                tp++;
                while (*tp != ',' &&
                       *tp != '\n' &&
                       *tp != '\0') {
                    *(tnumbuf++) = *(tp++);
                }
                *tnumbuf = '\0';
                double dest_weight = strtod(numbuf, &tnumbuf);
                tnumbuf = numbuf;
                memset(numbuf, 0, 64);

                if (*tp == ',') tp++;

                // PLZ fix for some reason this completely crashes everything and I can't figure out why
                add_edge(nodeid, dest_nodeid, dest_weight);

                if (++i %100 == 0) {
                    if (myrank == 0)  printf("%d\r",i);
                    fflush(stdout);
                }
            }
        }
    }

    void make_dummy_graph() {
        const unsigned int nodes_per_worker = MAX_GRAPH_NODES / this->commsize;
        const unsigned int node_id_offset = this->myrank * nodes_per_worker;
        const double edge_value = 1.0 / DEBUG_NUM_EDGES;
        for(unsigned int node_num = 0; node_num < nodes_per_worker; ++node_num) {
            int node_id = node_num + node_id_offset;
            add_vertex(node_id, DEFAULT_VERTEX_VALUE);
            unsigned int edge_num;
            for(edge_num = 0; edge_num < DEBUG_NUM_EDGES; ++edge_num) {
                add_edge(node_id, 10+edge_num, edge_value);
            }
        }
    }
public:
    void load_graph(const std::string& input_file) {
        // Get MPI data
        this->myrank = Pregel::get_worker_id();
        this->commsize = Pregel::get_num_workers();
        unsigned long long time_start, time_end;
        time_start = Metrics::get_cycle_time();
#if DUMMY_GRAPH
        make_dummy_graph();
#else
        if (myrank == 0) {
            std::cerr << "Loading graph from file " << input_file << std::endl;
        }
        load_graph_mpi(input_file);
#endif
        time_end = Metrics::get_cycle_time();
        Metrics::Counters::load_graph_time = time_end - time_start;
    }
};

// Responsible for writing results to filesystem
class BTCGraphDumper:public Pregel::BaseGraphDumper<BTCVertex> {
    void store_vertices(const std::string& file_path, const std::vector<BTCVertex>& vertices) {
        std::ofstream out_file(file_path.c_str(), std::ios::out);
        out_file << "vertex_id,group_id" << std::endl;
        for(unsigned int index = 0; index < vertices.size(); ++index) {
            out_file << vertices[index].id() << "," << vertices[index].value() << std::endl;
        }
        out_file.close();
    }
public:
    void dump_partition(const std::string& output_file, const std::vector<BTCVertex>& vertices) {
        // Suggest just having separate file per partition
        if(id() == 0) {
            unsigned long long write_begin = Metrics::get_cycle_time();
            store_vertices(output_file, vertices);
            Metrics::Counters::store_vertices_time = Metrics::get_cycle_time() - write_begin;
        }
    }
};

void write_metrics(const std::string& out_file_path) {
    // Add suffix to path
    const std::string metrics_out_path = (out_file_path + ".metrics");
    std::cout << "Writing out metrics to " << metrics_out_path << std::endl;
    std::ofstream out_file(metrics_out_path.c_str(), std::ios::out);
    out_file << "start-time,end-time,run-start-time,run-end-time,load-graph-time,total-compute-time,store-vertices-time" << std::endl;
    out_file << Metrics::Counters::start_time << ",";
    out_file << Metrics::Counters::end_time << ",";
    out_file << Metrics::Counters::run_start_time << ",";
    out_file << Metrics::Counters::run_end_time << ",";
    out_file << Metrics::Counters::load_graph_time << ",";
    out_file << Metrics::Counters::total_compute_time << ",";
    out_file << Metrics::Counters::store_vertices_time << std::endl;
    out_file.close();
}

int main(int argc, char ** argv) {
    // Initialize pregel library (wraps MPI)
    Pregel::init_pregel(argc, argv);
    if(argc != 6) {
        std::cerr << "Invalid input. Require ./main.out [in file path] [number of partitions] [max-iterations] [p (where probability of starting walk = p * (iteration-num / max-iterations)) [out file path]" << std::endl;
        return EXIT_FAILURE;
    }
    std::string in_file_path(argv[1]);
    const unsigned int num_partitions = atoi(argv[2]);
    BTCSettings::max_iterations = atoi(argv[3]);
    BTCSettings::p = atof(argv[4]);
    const std::string out_file_path(argv[5]);
    Metrics::Counters::start_time = Metrics::get_cycle_time();
    // Create worker
    Pregel::Worker<BTCVertex, BTCGraphLoader, BTCGraphDumper> worker;
    // Generate worker parameter struct
    Pregel::WorkerParams worker_params;
    worker_params.num_partitions = num_partitions;
    worker_params.input_file = in_file_path;
    worker_params.output_file = out_file_path;
    Metrics::Counters::run_start_time = Metrics::get_cycle_time();
    // Begin computation
    worker.run(worker_params);
    Metrics::Counters::run_end_time = Metrics::get_cycle_time();
    Metrics::Counters::end_time = Metrics::get_cycle_time();
    if(Pregel::get_worker_id() == 0) {
        write_metrics(out_file_path);
    }
    return EXIT_SUCCESS;
}
