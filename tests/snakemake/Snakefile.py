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
