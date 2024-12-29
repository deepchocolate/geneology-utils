import pandas as pd
import numpy as np
import graph_tool.all as gt
import gzip

pedigree_path = str(snakemake.input.pedigree)
out_path = str(snakemake.output.batch)
#
colInd = snakemake.params.col_ind
colMother = snakemake.params.col_mother
colFather = snakemake.params.col_father

max_gen_updown = int(snakemake.params.max_gen_updown)

n = int(snakemake.wildcards.n)

nsplit = int(snakemake.params.nsplit)
# read in the pedigree as a data table and put it in the right format
# pedigree should include lines for each ind listed as parent of other inds
# if parent is missing put value -1 in the parent1 / parent2 column 
# we need three columns, id of the target ind, and the ids of the two parents
pedigree = pd.read_hdf(pedigree_path)
#print(pedigree)
pedigree = pedigree[[colInd, colMother, colFather]].copy()

idx_of_id = dict(zip(
    pedigree[colInd].values,
    pedigree.index.values
    )
)
idx_of_id['0'] = -1
idx_of_id[0] = -1
idx_of_id[-1] = -1

# nodes in the graph will be indexed / labeled from 0 and counting up by 1 
# we assign each ind an index, and assign them to node with that index
parent1_of_idx = dict(zip(pedigree.index.values,
                       [idx_of_id[x] for x in pedigree[colMother]]))
parent2_of_idx = dict(zip(pedigree.index.values,
                       [idx_of_id[x] for x in pedigree[colFather]]))

pedigree['parent1_idx'] =  [idx_of_id[x] for x in pedigree[colMother]]
pedigree['parent2_idx'] =  [idx_of_id[x] for x in pedigree[colFather]]

# setup directed edges from child -> parent
z = [(x.Index, x.parent1_idx) for x in pedigree.itertuples() if x.parent1_idx >= 0]
y = [(x.Index, x.parent2_idx) for x in pedigree.itertuples() if x.parent2_idx >= 0]

# make the actual directed graph with edges going from the child to the parent
# up to two edges for each row in the pedigree
graph = gt.Graph(y+z, directed=True)
#gt.graph_draw(graph, output='test.png')
# here I have split up for multiprocessing, but not really necessary 
splits = np.array_split(np.arange(graph.num_vertices()), nsplit)
#splits = np.arange(graph.num_vertices())

anc = list()
anc2 = list()
# for each node (ind)
for i in splits[n]:
    # distance to all other nodes
    dist_map = graph.new_vp("int16_t", val=np.iinfo(np.int16).max)
    dist = gt.shortest_distance(
        graph,
        source=i,
        target = None,
        dist_map = dist_map,
        return_reached = False
    )
    arr = dist.a
	
    # record distances above 0 and below the threshold (e.g. 12)
    ancestors = np.where(
        np.logical_and(
            arr < max_gen_updown, arr > 0
        )
    )[0]
    
    for a in ancestors:
        anc2.append((pedigree[colInd][i], pedigree[colInd][a], arr[a]))
        anc.append((i, a, arr[a]))
        

# output data frame with one row per child-ancestor relationship, 
# also include the distance in generations
anc = pd.DataFrame(anc)
#print(anc)
anc2 = pd.DataFrame(anc2)
#print(anc2)
#print(out_path)
#for j in anc.itertuples():
#	print(j[0])

#for i in anc:
#	print(anc[i])
#if len(anc2) > 0: 
if len(anc2) == 0: anc2 = pd.DataFrame({'child':[], 'ancestor':[], 'ngen':[]})
columns = ['child', 'ancestor', 'ngen']
anc2.to_csv(out_path, sep='\t', index=None, header=columns)
#with open(out_path, 'rb') as f:
#	print(f.read())
#if len(anc)>0:
#	pass
    #anc.to_csv(out_path, sep = '\t', index=None, header=None)
#else:
#    with gzip.open(out_path, "w") as fh:
#        pass
