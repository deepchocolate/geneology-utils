sudo singularity build -B $PWD:/home pydigree.sif pydigree.def
conda run -n pydigree python --version
