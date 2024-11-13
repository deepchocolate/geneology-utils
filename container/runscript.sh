# When executing the container using this script, the first argument should
# be to conda environment to run in. Running "python myscript.py" should thus be:
# singularity run gen.sif <environment> python myscript.py
eval "$(/conda/bin/conda shell.bash hook);"
source activate base
conda activate $1
${@:2}
