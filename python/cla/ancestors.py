import pandas as pd
import numpy as np
import graph_tool.all as gt
import gzip
from io import StringIO
import os
import multiprocessing as mp
import tracemalloc

class Ancestors:
	
	fileInput = None
	fileIntermediate = None
	fileOutput = None
	maxGen = None
	sort = None
	dictCols = {'ID':'ID', 'MOTHER':'MOTHER', 'FATHER':'FATHER'}
	
	def __init__(self, fileInput, fileIntermediate, fileOutput, maxGen, sort=None, dictCols=None):
		self.fileInput = fileInput
		self.fileIntermediate = fileIntermediate
		self.fileOutput = fileOutput
		self.maxGen = maxGen
		self.sort = sort
		if dictCols != None: self.dictCols = dictCols
	
	@staticmethod
	def run(args):
		tracemalloc.start()
		fIntermediate = args.file_intermediate
		dictCols = {'ID':args.col_individual, 'MOTHER':args.col_mother, 'FATHER':args.col_father}
		a = Ancestors(args.file_input, fIntermediate, args.file_output, args.generations_max, args.sort, dictCols)
		if not os.path.isfile(fIntermediate): a.createFileIntermediate()
		cores = args.cores
		if cores == None:
			cores = mp.cores = mp.cpu_count() - 1
		a.get(cores)
		mr = tracemalloc.get_traced_memory()
		print(mr[1]/1024)
		tracemalloc.stop()
	
	def createFileIntermediate(self):
		df = pd.read_csv(StringIO(self.fileInput.read()), sep='\t')
		hdfFile = pd.HDFStore(self.fileIntermediate)
		hdfFile.put('trio', df, format='table', data_columns=True)
		hdfFile.close()
	
	@staticmethod
	def getAncestors(dta, indices, maxGen, anc, colInd):
		graph = dta[0]
		trios = dta[1]
		for i in indices:
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
					arr < maxGen, arr > 0
				)
			)[0]
			#anc.append(ancestors)
			for a in ancestors:
				anc.append((trios[colInd][i], trios[colInd][a], arr[a]))
		return ancestors
	
	def get(self, nSplits):
		colInd = self.dictCols['ID']
		colMother = self.dictCols['MOTHER']
		colFather = self.dictCols['FATHER']
		trios = pd.read_hdf(self.fileIntermediate)
		trios = trios[[colInd, colMother, colFather]].copy()
		# ID and index of offspring
		idIndex = dict(zip(trios[colInd].values, trios.index.values))
		idIndex['0'] = -1
		idIndex[0] = -1
		idIndex[-1] = -1
		# Mothers of index individuals
		idMothers = dict(zip(trios.index.values, [idIndex[x] for x in trios[colMother]]))
		idFathers = dict(zip(trios.index.values, [idIndex[x] for x in trios[colFather]]))
		# Add index to parents
		trios['mother_index'] = [idIndex[x] for x in trios[colMother]]
		trios['father_index'] = [idIndex[x] for x in trios[colFather]]
		z = [(x.Index, x.mother_index) for x in trios.itertuples() if x.mother_index >= 0]
		y = [(x.Index, x.father_index) for x in trios.itertuples() if x.father_index >= 0]
		graph = gt.Graph(y+z, directed=True)
		splits = np.array_split(np.arange(graph.num_vertices()), nSplits)
		jobs = []
		man = mp.Manager()
		anc = man.list()
		dta = man.list()
		dta.append(graph)
		dta.append(trios)
		for i in range(0, len(splits)):
			p = mp.Process(target=Ancestors.getAncestors, args=(dta, splits[i], self.maxGen, anc, colInd))
			jobs.append(p)
			p.start()
		for i in jobs:
			i.join()
		anc = list(anc)
		#anc2 = list()
		# for each node (ind)
		#for i in splits[splitIndex]:
			# distance to all other nodes
		#	dist_map = graph.new_vp("int16_t", val=np.iinfo(np.int16).max)
		#	dist = gt.shortest_distance(
		#		graph,
		#		source=i,
		#		target = None,
		#		dist_map = dist_map,
		#		return_reached = False
		#		)
		#	arr = dist.a
	
			# record distances above 0 and below the threshold (e.g. 12)
		#	ancestors = np.where(
		#		np.logical_and(
		#			arr < self.maxGen, arr > 0
		#		)
		#	)[0]
		#	for a in ancestors:
		#		anc2.append((trios[colInd][i], trios[colInd][a], arr[a]))
		anc = pd.DataFrame(anc)
		anc = anc.rename(columns={0:'child', 1:'ancestor', 2:'ngen'})
		#if len(anc) == 0: anc = pd.DataFrame({'child':[], 'ancestor':[], 'ngen':[]})
		if self.sort != None:
			sort = self.sort.split(' ')
			anc = anc.sort_values(sort)
		columns = ['child', 'ancestor', 'ngen']
		anc.to_csv(self.fileOutput, sep='\t', index=None, header=columns)
