#include"Pregel.h"
#include<iostream>
#include<limits.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>

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
	inline void combine(BTCMessage& old_message, BTCMessage& new_message) {
		//No-op (vertex joins only one walk)
	}
};

namespace BTCSettings {
	unsigned int max_iterations;
	double p;
};



class BTCVertex:public Pregel::BaseVertex<VertexValue, VertexEdge, BTCMessage, Pregel::DefaultHash, BTCCombiner> {
private:
	typedef Pregel::BaseVertex<VertexValue, VertexEdge, BTCMessage, Pregel::DefaultHash, BTCCombiner> BaseVertexType;
public:
    BTCVertex():BaseVertexType(){};
	BTCVertex(Pregel::VertexID id):BaseVertexType(id){};
	BTCVertex(Pregel::VertexID id, VertexValue value):BaseVertexType(id, value){};

	//TODO: check for a message, pick first message, join that thing's group if there's a message
	//If no message, randomly decide (see formula in main) whether to start walk
	//If now in a walk, randomly pick a child to send to based on edge weights
	//Once part of a group and after sending message to child, call vote_for_halt()
    void compute(const MessageContainer& messages) {
        if(step_num()>=(int)BTCSettings::max_iterations){
			//TODO: assign self to own walk
			Pregel::vote_for_halt();
			return;
		}
    }
};

#define DUMMY_GRAPH 1
#define MAX_GRAPH_NODES 200000
#define MAX_GRAPH_EDGES 10000
#define MAX_DEGREE MAX_GRAPH_NODES / MAX_GRAPH_EDGES
#define DEFAULT_VERTEX_VALUE -1 // Default vertex value is -1 (not in walk)

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
		for(unsigned int node_num = 0; node_num < nodes_per_worker; ++node_num) {
			int node_id = node_num + node_id_offset;
			add_vertex(node_id, DEFAULT_VERTEX_VALUE);
			for(unsigned int edge_num = 0; edge_num < MAX_DEGREE; ++edge_num) {
				add_edge(node_id, (node_id + rand() * node_id_offset) % MAX_GRAPH_NODES, 1.0 / MAX_DEGREE);
			}	
			//TODO: add edges
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
