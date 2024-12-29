import pandas as pd
from pandas import HDFStore
DATASETS=["00", "01"]
SPLITS = [x for x in range(0, 2)]
rule relatives:
  input: matched_ancestors="output/matched_ancestors.tsv"
  output: assigned_relatives="output/assigned_relatives.csv", relative_table="output/relative_table.csv"
  script: "../scripts/assign_pedigree_relatives.py"

rule matchAncestors:
  input:
   ancestors="output/population-ancestors.csv",
   splits=expand("output/population-ancestors{n}.csv", n=SPLITS)
  output: relatives="output/matched_ancestors.tsv"
  script: "../scripts/merge_pedigree_ancestors.py"

rule mergeAncestors:
  input: file=expand("output/population-ancestors{n}.csv", n=SPLITS)
  output: file="output/population-ancestors.csv"
  shell: 
   r"""
   tail -n +2 -q {input.file} >> {output.file}
   """

rule getanc:
  input: pedigree="output/population.p5"
  output: batch="output/population-ancestors{n}.csv"
  params:
    max_gen_updown=10,
    nsplit=len(SPLITS),
    col_ind='ID',
    col_mother='MOTHER',
    col_father='FATHER'
  script: "../scripts/get_ancestors.py"
   
rule convert:
  input: "output/population.tsv"
  output: pedigree="output/population.p5"
  run:
    for f in input:
      hdfFile = HDFStore(output[0])
      df = pd.read_csv(f, sep='\t')
      hdfFile.put('test', df, format='table', data_columns=True)

rule merge:
  input:
    comps="output/faily-components-kombi.csv",
    popn="output/population-kombi-withfounders.tsv"
  output: file="output/population-famid-kombi.tsv"
  shell:
    r"""
    Rscript merge.sh {input.comps} {input.popn} {output.file}
    """

rule components:
  input: trios="output/offspring-parent-kombi.csv"
  output: geneologies="output/family-components-kombi.csv"
  script: "scripts/getcomponents.py"

rule shuffle:
  input: file="output/population-kombi-withfounders.tsv"
  output: file="output/offspring-parent-kombi.csv"
  shell:
    r"""
    tail -n +2 -q {input.file} | cut -f 1,2 --output-delimiter=, >> {output.file}
    tail -n +2 -q {input.file} | cut -f 1,3 --output-delimiter=, >> {output.file}
    """

rule addfounders:
  input: file="data/population-kombi.tsv"
  output: file="output/population-kombi-withfounders.tsv"
  shell:
    r"""
    Rscript scripts/add-founders.R --file-input {input.file} --file-output {output.file}
    """
