all: main.cpp
	mpic++ main.cpp -o miner.out -I ../../lib -Wall -Wno-deprecated
test60:
	mpirun -np 60 ./miner.out
clean:
	rm *.out
