#include"Pregel.h"
#include<iostream>
#include<vector>
#include<algorithm>
#include<limits.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>

#define DUMMY_GRAPH 1
#define MAX_GRAPH_NODES 10000
#define MAX_GRAPH_EDGES 10000
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
        std::cout << "Combining " << old_message << " and " << new_message << std::endl;
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
        const double random_number = (double) rand() / RAND_MAX;
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
        for(unsigned int invite_num = 0; invite_num < BTCSettings::branching_factor && invite_num < weights.size(); ++invite_num) {
            unsigned int chosen_index = select_weighted_index(weights);
            //TODO: zero out child and renormalize remaining weights to avoid sending to same child twice
            Pregel::VertexID target = edges().at(chosen_index).target;
            send_message(target, value);
        }
    }
    // Decide whether to initiate a walk
    bool start_walk() {
        if(step_num() >= (int)BTCSettings::max_iterations) { return true; }
        const double prob_start = BTCSettings::p * (((double)step_num()) * ((double)BTCSettings::max_iterations));
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
        // Choose message if message, otherwise randomly decide to start walk using vertex id
        if(messages.empty() == false) {
            value() = messages.front();
        } else if (start_walk()) {
            value() = id();
        }

        //std::cout << "Number of edges in " << id() << ": " << edges().size() << std::endl;

        if(value() != DEFAULT_VERTEX_VALUE) {
            //TODO: logging
            send_to_children(value());
            Pregel::vote_for_halt();
        }   
    }
};

#define DEBUG_NUM_EDGES 20

class BTCGraphLoader:public Pregel::BaseGraphLoader<BTCVertex>{
private:
    int myrank;
    int commsize;

    void load_graph_mpi(const std::string& input_file) {
        //TODO
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
                add_edge(node_id, (rand() / RAND_MAX * MAX_GRAPH_NODES), edge_value);
            }
        }
    }
public:
    void load_graph(const std::string& input_file) {
        // Get MPI data
        this->myrank = Pregel::get_worker_id();
        this->commsize = Pregel::get_num_workers();
#if DUMMY_GRAPH
        make_dummy_graph();
#else
        load_graph_mpi(input_file);
#endif
        }
};

// Responsible for writing results to filesystem
class BTCGraphDumper:public Pregel::BaseGraphDumper<BTCVertex> {
public:
    void dump_partition(const std::string& output_file, const std::vector<BTCVertex>& vertices) {
        //TODO: write vertex properties (vertex id and vertex walk id) to file
        // Suggest just having separate file per partition
    }
};

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
    // Create worker
    Pregel::Worker<BTCVertex, BTCGraphLoader, BTCGraphDumper> worker;
    // Generate worker parameter struct
    Pregel::WorkerParams worker_params;
    worker_params.num_partitions = num_partitions;
    worker_params.input_file = in_file_path;
    worker_params.output_file = out_file_path;
    // Begin computation
    worker.run(worker_params);
    return EXIT_SUCCESS;
}
