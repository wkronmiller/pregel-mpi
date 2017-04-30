#include"Pregel.h"
#include<iostream>

#include<limits.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>

using namespace Pregel;

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



class BTCVertex:public Pregel::BaseVertex<VertexValue, VertexEdge, BTCMessage, DefaultHash, BTCCombiner> {
private:
	typedef Pregel::BaseVertex<VertexValue, VertexEdge, BTCMessage, DefaultHash, BTCCombiner> BaseVertexType;
	unsigned int max_iterations;
	double p;
public:
    BTCVertex(VertexID id, const long long int color): BaseVertexType(id, color){};
    BTCVertex(VertexID id){};
    BTCVertex():BaseVertexType(){};
	// Use this constructor in load_graph
	BTCVertex(VertexID id, const unsigned int& max_iterations, const double& p):BaseVertexType(id, -1){
		this->max_iterations = max_iterations;
		this->p = p;
	};

    void compute(const MessageContainer& messages) {
		//TODO: check for a message, pick first message, join that thing's group if there's a message
		//If no message, randomly decide (see formula in main) whether to start walk
		//If now in a walk, randomly pick a child to send to based on edge weights
		//Once part of a group and after sending message to child, call vote_for_halt()

        if(step_num()>=30){
			vote_for_halt();
			return;
		}
    }
};

class BTCGraphLoader:public Pregel::BaseGraphLoader<BTCVertex>{
public:
	void load_graph(const std::string & input_file) {
		const int myrank = get_worker_id();
		const int commsize = get_num_workers();

        // Stolen from btc-graph-miner sorta
        MPI_Offset file_size;
        MPI_File fh;
        int err;

        err = MPI_File_open(MPI_COMM_WORLD, input_file.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
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
        if (myrank == 3) sleep(1);
        while (*tp != '\0') {
            // Grab the node id
            while (*tp != ';') {
                *(tnumbuf++) = *(tp++);
                if (*tp == '\0') break;
            }
            if (*tp == '\0') break;
            *tnumbuf = '\0';
            tnumbuf = numbuf;
            long long int nodeid = atoll(numbuf);
            tp++;
            add_vertex(nodeid, (long long int)0);
            while (*tp != '\n' && *tp != '\0') tp++;
            if (tp[1] == '\0') break;

            // Just status monitoring - remove on AMOS
            if (++i %100 == 0) {
                printf("%d\r",i);
                fflush(stdout);
            }
        }

        // Wait to ensure that all the verticies exist before adding edges
        MPI_Barrier(MPI_COMM_WORLD);

        tp = buffer;
        while (*tp != '\n') tp++;
        i == 0;
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
	const unsigned int max_iterations = atoi(argv[3]);
	const double p = atof(argv[4]);
	const std::string out_file_path(argv[5]);
	// Create worker
	Worker<BTCVertex, BTCGraphLoader, BTCGraphDumper> worker;
	// Generate worker parameter struct
	Pregel::WorkerParams worker_params;
	worker_params.num_partitions = num_partitions;
	worker_params.input_file = in_file_path;
	worker_params.output_file = out_file_path;
	// Begin computation
	worker.run(worker_params);
	return EXIT_SUCCESS;
}
