#!/bin/bash

ASSIGN_PATH=scratch/assignmentFinal/

if [ -z "$BGQ_USER" ]; then
    BGQ_USER=PCP6krnm
    echo "Defaulted to bgq user $BGQ_USER"
fi

if [ -z "$Q_LOCAL" ]; then
    Q_LOCAL=$BGQ_USER@lp04.ccni.rpi.edu
    echo "Defaulted to local bgq alias $Q_LOCAL"
fi


rsync -rtuvz --exclude '*.out' --exclude "*.dSym/" --exclude "*.tmp" --exclude "*.swp" --exclude '.git' ../ $Q_LOCAL:/gpfs/u/home/PCP6/$BGQ_USER/$ASSIGN_PATH
