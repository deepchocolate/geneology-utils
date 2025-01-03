import pandas as pd
from pandas import HDFStore
dirOutput = 'snakemake/output'
DATASETS=["00", "01"]
SPLITS = [x for x in range(0, 2)]
rule relatives:
  input: matched_ancestors=dirOutput + "/matched_ancestors.tsv"
  output: assigned_relatives=dirOutput + "/assigned_relatives.csv", relative_table=dirOutput + "/relative_table.csv"
  script: "scripts/assign_pedigree_relatives.py"

rule matchAncestors:
  input:
   ancestors=dirOutput + "/population-ancestors.csv",
   splits=expand(dirOutput + "/population-ancestors{n}.csv", n=SPLITS)
  output: relatives=dirOutput + "/matched_ancestors.tsv"
  script: "scripts/merge_pedigree_ancestors.py"

rule mergeAncestors:
  input: file=expand(dirOutput + "/population-ancestors{n}.csv", n=SPLITS)
  output: file=dirOutput + "/population-ancestors.csv"
  shell: 
   r"""
   tail -n +2 -q {input.file} >> {output.file}
   """

rule getanc:
  input: pedigree=dirOutput + "/population.p5"
  output: batch=dirOutput + "/population-ancestors{n}.csv"
  params:
    max_gen_updown=10,
    nsplit=len(SPLITS),
    col_ind='ID',
    col_mother='MOTHER',
    col_father='FATHER'
  script: "scripts/get_ancestors.py"
   
rule convert:
  input: dirOutput + "/population-famid.tsv"
  output: pedigree=dirOutput + "/population.p5"
  run:
    for f in input:
      hdfFile = HDFStore(output[0])
      df = pd.read_csv(f, sep='\t')
      hdfFile.put('test', df, format='table', data_columns=True)

rule merge:
  input:
    comps=dirOutput + "/family-components.csv",
    popn=dirOutput + "/population-withfounders.tsv"
  output: file=dirOutput + "/population-famid.tsv"
  shell:
    r"""
    Rscript ../scripts/R/merge.sh {input.comps} {input.popn} {output.file}
    """

rule components:
  input: trios=dirOutput + "/offspring-parent.csv"
  output: geneologies=dirOutput + "/family-components.csv"
  params:
	  sep = ','
  script: "scripts/getcomponents.py"

rule shuffle:
  input: file=dirOutput + "/population-withfounders.tsv"
  output: file=dirOutput + "/offspring-parent.csv"
  shell:
    r"""
    tail -n +2 -q {input.file} | cut -f 1,2 --output-delimiter=, >> {output.file}
    tail -n +2 -q {input.file} | cut -f 1,3 --output-delimiter=, >> {output.file}
    """

rule addfounders:
  input: file="input/trios.tsv"
  output: file=dirOutput + "/population-withfounders.tsv"
  shell:
    r"""
    Rscript ../scripts/R/add-founders.R --file-input {input.file} --file-output {output.file}
    """
