#include"Pregel.h"
#include<iostream>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>

// Default checked function error-handler
void handleError(int err, int myrank) {
    if (err) {
        char estr[256] = {0};
        int len = 0;
        MPI_Error_string(err, estr, &len);
        fprintf(stderr,"[%u]: MPI error: %s\n", myrank, estr);
		sleep(10); // Sleep to give MPI time to log message
		MPI_Abort(MPI_COMM_WORLD, err);
    }
}

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

        // Stolen from btc-graph-miner sorta
        MPI_Offset file_size;
        err = MPI_File_get_size(fh, &file_size);
        handleError(err, myrank);

        const int rank_chunk_size = (file_size / commsize);
        const MPI_Offset start_offset = rank_chunk_size * myrank;
        MPI_Offset inner_end_offset = start_offset + rank_chunk_size;

        if(file_size - inner_end_offset < rank_chunk_size) {
            inner_end_offset = file_size;
        }
        const size_t buffer_size = inner_end_offset - start_offset - current_position;
        const int outer_buf_max = 32768;
        char* buffer = (char*)calloc(sizeof(char), (buffer_size + 1 + outer_buf_max));
        buffer[buffer_size] = '\0';

        // Don't think there's much risk of this not getting everything...
        err = MPI_File_read(fh, buffer, bytes_to_read, MPI_CHAR, &read_status);
        handleError(err, myrank);

        // Find where the next newline is.
        char* buffer_pointer = buffer + bytes_to_read;
        err = MPI_File_read(fh, buffer_pointer, outer_buf_max, MPI_CHAR, &read_status);
        handleError(err, myrank);

        while (*buffer_pointer != '\n') {
            buffer_pointer++;
        }
        *buffer_pointer = '\0';

        buffer_pointer = buffer;
        char* tp = buffer_pointer;
        char numbuf[64];
        char* tnumbuf = numbuf;
        while (*buffer_pointer != '\0') {
            // Grab the node id
            while (*buffer_pointer != ';') *(tnumbuf++) = *(buffer_pointer++);
            *tnumbuf = '\0';
            tnumbuf = numbuf;
            int nodeid = atoi(numbuf);
            // ADD VERTEX HERE

            while (*buffer_pointer != '\n' && *buffer_pointer != '\0') {
                while (*buffer_pointer != ':') *(tnumbuf++) = *(buffer_pointer++);
                *tnumbuf = '\0';
                tnumbuf = numbuf;
                int dest_nodeid = atoi(numbuf);

                while (*buffer_pointer != ',' &&
                       *buffer_pointer != '\n' &&
                       *buffer_pointer != '\0')
                    *(tnumbuf++) = *(buffer_pointer++);
                *tnumbuf = '\0';
                tnumbuf = numbuf;
                double dest_weight = atof(numbuf);
                // ADD EDGE HERE

            }
        }
    }
};

int main(int argc, char ** argv) {
	Pregel::init_pregel(argc, argv);
	std::cout << "Hello world: " << Pregel::_my_rank << std::endl;
	return EXIT_SUCCESS;
}
