#!/bin/bash
echo "$0"
dr=$(dirname "$0")/
if [ "$dr" = "./" ]; then dr=$PWD; fi
sudo apptainer build -B $PWD:/home gen.sif gen.def
