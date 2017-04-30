all: main.cpp
	mpic++ -std=c++98 main.cpp -o miner.out -isystem ../../lib -Wall -Wno-deprecated --std=c++11
test60:
	mpirun -np 60 ./miner.out
clean:
	rm *.out
