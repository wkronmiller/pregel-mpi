#!/bin/bash
squeue | grep -Po '^ *([0-9]*)' | grep -Po '[0-9]*' | xargs scancel
