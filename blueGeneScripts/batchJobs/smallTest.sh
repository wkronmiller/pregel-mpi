#!/bin/bash
# To be run on 64 nodes
INPUT_FILE=/gpfs/u/home/PCP6/PCP6krnm/scratch/assignmentFinal/groups_200k_new
MAX_ITERATIONS=1000
P_PROB=$1
RESULTS_DIR=/gpfs/u/home/PCP6/PCP6krnm/scratch/assignmentFinal/results
RESULTS_PREFIX=smalljob-synthetic

for NUM_NODES in 8 16 32
do
    NUM_TASKS=$((NUM_NODES * 16 * 2))
    OUT_PATH="$RESULTS_DIR/$RESULTS_PREFIX-$NUM_NODES-nodes-$NUM_TASKS-tasks-$P_PROB-prob"
    srun --overcommit -N $NUM_NODES --ntasks $NUM_TASKS -o "smallJob$NUM_NODES.log" ../../apps/btcMiner/miner.out $INPUT_FILE $NUM_TASKS $MAX_ITERATIONS $P_PROB $OUT_PATH &
done
wait
echo "$RESULTS_PREFIX done"
