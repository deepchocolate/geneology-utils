#!/bin/bash
#tail -n +2 input/trios.tsv | cut -f 1,2,3 > output/edge_list.txt
#tail -n +2 input/family-membership.tsv > output/edge_list.txt
# Remove the header
dirBind=$(git rev-parse --show-toplevel)
dirTests=$dirBind/tests
SIF=$dirBind/container/gen.sif
singularity run -B $dirTests $SIF pedigree-utils python3 -m unittest --verbose tests.py
exit
singularity exec -B $dirTests $SIF Rscript $dirTests/create-test-data.R
tail -n +2 input/offspring-parent.tsv > output/edge_list.txt
singularity exec -B $dirBind $SIF Rscript generate-pedigrees.R --file-input output/population.tsv --col-pid FAMID --file-output output/pedigrees.Rdata
singularity run -B $dirBind:$dirBind $SIF graphtool snakemake -c1 -s Snakefile-components
singularity exec $SIF Rscript ../scripts/R/add-founders.R --file-input input/trios.tsv  --file-output output/population.tsv
singularity run -B $dirBind:$dirBind $SIF graphtool snakemake -c1 -s Snakefile
