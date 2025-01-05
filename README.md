# geneology-utils

A collection of python scripts and command-line tools to process multi-generational
data.

## Prerequisites
The apptainer definition file `container/gen.def` outlines the necessary
software. The simplest approach is to build a container from this definition file.
By running `sh container/build.sh` a container named `gen.sif` will be
created inside the container directory.

That everything runs as expected can be tested by executing the tests.
```
apptainer exec -B $PWD container/gen.sif sh tests/run-tests.sh
```

`geneology-utils` is embedded in the container and can be executed directly:
```
apptainer exec -B $PWD container/gen.sif geneology-utils --help
```


## Input
The input data consist of multigenerational data with offspring, mother,
and father ("trio structure").
```
OFFSPRING MOTHER FATHER
O1	F1	F2
O2	F3	F4
O3	F5	-
O4	O1	O2
```

## Output
```
descendant_x descendant_y kinship_sum n_shared_anc gsep_x gsep_y rel
O4	01	0.25	1	0	1	0	PO
```

## Computational requirements
This tool has currently been applied to a population of 8M individuals.
In total this took about 24 hours using 30 cores and 1.5GB or memory per core.
