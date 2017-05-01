#!/bin/bash

for P_PROB in 0.1 0.5 1 1.5
do
    sbatch --partition medium --nodes 512 --time 8 ./allInOne.sh $P_PROB
done
