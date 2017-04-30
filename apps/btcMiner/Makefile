in_file = /mnt/ebsmag1/groups_200k_new
num_partitions = 1
max_iterations = 10000
p_prob = 0.9
out_path = /mnt/ebsmag1/testOutputs/out.pregel

all: main.cpp
	mpic++ -std=c++98 -g -lSegFault main.cpp -o miner.out -isystem ../../lib -Wall -Wno-deprecated
	mpic++ -fsanitize=address -std=c++98 -g -lSegFault main.cpp -o debug.out -isystem ../../lib -Wall -Wno-deprecated

test1: all
	./miner.out $(in_file) $(num_partitions) $(max_iterations) $(p_prob) $(out_path)

test6: all
	mpirun -np 6 ./miner.out $(in_file) $(num_partitions) $(max_iterations) $(p_prob) $(out_path)

test60: all
	mpirun -np 60 ./miner.out $(in_file) $(num_partitions) $(max_iterations) $(p_prob) $(out_path)

clean:
	rm *.out
