#!/usr/bin/env python3
# This script is based on github.com/yorugosu/genealogy/blob/main/components.py
import networkx as nx
import pandas as pd

fileInput = snakemake.input.trios
fileOutput = snakemake.output.geneologies
sepFileInput = snakemake.params.sep

# Read the edge list
G = nx.read_edgelist(fileInput, nodetype=int, data=(("weight", float),), delimiter=sepFileInput)

# Identify graph components
components = nx.connected_components(G)

indid = []
sizes = []
for component in components:
	indid.extend(component)
	sizes.append(len(component))

famid = []
for i in range(len(sizes)):
	famid.extend([i+1]*sizes[i])

ids = pd.DataFrame({'famid':famid, 'indid':indid})
ids.to_csv(fileOutput, sep=',', index=None, header=True)
