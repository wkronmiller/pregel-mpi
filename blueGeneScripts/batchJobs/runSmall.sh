#!/bin/bash

for P_PROB in 0.1 0.5 0.8 0.9 1 1.2 1.5
do
    sbatch --partition small --nodes 64 --time 10 ./smallTest.sh $P_PROB
done
