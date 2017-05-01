in_file = /mnt/ebsmag1/groups_200k_new
num_partitions = 1
max_iterations = 10
p_prob = 1.1
out_path = /mnt/ebsmag1/testOutputs/out.pregel

all: main.cpp
	mpic++ -std=c++98 main.cpp -o miner.out -I ../../lib -Wall -Wno-deprecated -ggdb

bluegenecompile: main.cpp
	mpic++ -std=c++98 -g main.cpp -D __BGQ__ -o miner.out -I ../../lib
    
bluegenerun:
	srun --overcommit --partition debug --time 8 --ntasks 60 ./miner.out /gpfs/u/home/PCP6/PCP6krnm/scratch/assignmentFinal/groups_200k_new 1 100 1.1 /gpfs/u/home/PCP6/PCP6krnm/scratch/assignmentFinal/output.pregel

test1: all
	./miner.out $(in_file) $(num_partitions) $(max_iterations) $(p_prob) $(out_path)

test6: all
	mpirun -np 6 ./miner.out $(in_file) $(num_partitions) $(max_iterations) $(p_prob) $(out_path)

test60: all
	mpirun -np 60 ./miner.out $(in_file) $(num_partitions) $(max_iterations) $(p_prob) $(out_path)

clean:
	rm *.out
