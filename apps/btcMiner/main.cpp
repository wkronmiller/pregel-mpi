#include"Pregel.h"
#include<iostream>
#include<stdio.h>

// Adapted from pageRank example

typedef struct VertexValue {
	// ID of transaction group vertex represents
	unsigned long long group_id;
	// ID of walk vertex is a member of (zero means not a member yet)
	unsigned long long walk_id = 0;
};
typedef struct VertexEdge {
	// Markov probability value
	double weight;
};
typedef BTCMessage {
	unsigned long long walk_id;
};


class BTCCombiner:public Pregel::BaseCombiner<BTCMessage> {
public:
	inline void combine(BTCMessage& old_message, BTCMessage& new_message) {
		//No-op (vertex joins only one walk)
	}
};

class BTCVertex:public BaseVertex<VertexValue, VertexEdge, BTCMessage, DefaultHash, BTCCombiner> {
	//TODO
};

class BTCGraphLoader:public BaseGraphLoader<BTCVertex>{
public:
	void load_graph(const std::string & input_file) {
		const int myrank = get_worker_id();
		const int commsize = get_num_workers();
		//TODO: use MPI_File_read operations to load from file
	}
};

int main(int argc, char ** argv) {
	Pregel::init_pregel(argc, argv);
	std::cout << "Hello world: " << Pregel::_my_rank << std::endl;
	return EXIT_SUCCESS;	
}
