import pandas as pd
import numpy as np
import multiprocessing as mp
from cla.variables import REL
import tracemalloc

class Relatives:
	
	fileInput = None
	fileOutput = None
	cores = None
	
	def __init__(self, fileInput, fileOutput, cores):
		self.fileInput = fileInput
		self.fileOutput = fileOutput
		cores = self.cores
	
	@staticmethod
	def run(args):
		r = Relatives(args.file_input, args.file_output, args.cores)
		return r.get()
	
	@staticmethod
	def pairOnAncestry(i):
		splits[i].columns = ['descendant', 'ancestor', 'gsep']
		m = pd.merge(
			splits[i], splits[i], 
			on = 'ancestor', 
			how = 'inner',
			suffixes = ['_x', '_y'],
		)
		#print(m)
		m = m.query('descendant_x != descendant_y')  # get rid of descendant matches
		m = m[['descendant_x', 'descendant_y', 'gsep_x', 'gsep_y', 'ancestor']]
		#print(m)
		# above code does not record direct descendants
		# record them out to 5 generations
		#direct_ancestors = df.copy()
		rel_of_gsep = {1:'P0', 2: '1G', 3:'2G', 4:'3G', 5:'4G'}
		s = splits
		splits[i]['rel'] = splits[i]['gsep'].map(rel_of_gsep)
		rel_types = pd.CategoricalDtype(categories=['P0', '1G', '2G', '3G', '4G'], ordered=True)
		splits[i]['rel'].astype(rel_types)
		#direct_ancestors = s.sort_values(['descendant', 'ancestor'])
		#s = s.sort_values(['descendant', 'ancestor'])

		splits[i].columns = ['descendant_x', 'descendant_y', 'gsep_x', 'rel']
		splits[i]['gsep_y'] = 0
		splits[i]['ancestor'] = splits[i]['descendant_y']
		splits[i] = splits[i][['descendant_x', 'descendant_y', 'gsep_x', 'gsep_y', 'ancestor']]
		#print(splits[i])
		#splits[i] = pd.concat([m, splits[i]])
		return pd.concat([m, splits[i]])
	
	def get(self):
		tracemalloc.start()
		cores = self.cores
		if cores == None: cores = mp.cpu_count() - 1
		merge_in = pd.read_csv(self.fileInput, sep ='\t', header=0)
		# Split data by ancestor
		global splits
		splits = [x for __, x in merge_in.groupby('ancestor')]
		#print(splits)
		pool = mp.Pool(cores)
		matched_ancestors = pool.map(Relatives.pairOnAncestry, range(len(splits)))
		matched_ancestors = pd.concat(matched_ancestors)
		#print(res)
		#matched_ancestors = pd.concat(res)
		#splits = pd.concat(splits)
		#print(pd.concat(splits))
		#print(splits)
		#print(pd.concat(splits))
		#matched_ancestors = splits
		#print(res)
		#print(matched_ancestors)
		
		
		dtypes = {
			'descendant_x': np.int32,
			'descendant_y': np.int32,
			'gsep_x': np.int8,
			'gsep_y': np.int8,
			'ancestor': np.int32,
		}
		# find most recent common ancestor(s)
		matched_ancestors['gsep_t'] = matched_ancestors['gsep_x'] + matched_ancestors['gsep_y']
		mrca = matched_ancestors.groupby(['descendant_x', 'descendant_y'])[['gsep_t']].min().reset_index()
		mrca.columns = ['descendant_x', 'descendant_y', 'gsep_min']
		# only retain relationship at least this recent
		minrels = mrca.merge(matched_ancestors).query('gsep_t == gsep_min')
		# calculate kinship due to each shared ancestor
		# relatedness coefficient is 2x kinship coeffienct 
		minrels['kinship'] = np.power(0.5, minrels['gsep_t']+1)

		# find total kinship based on all shared ancestors
		# sum of each shared ancestor
		totalrel = minrels.groupby(
			['descendant_x', 'descendant_y'])[['kinship', 'gsep_x','gsep_y']].agg(
				{'kinship': ['sum', 'size'], 
				'gsep_x': lambda x: int(np.mean(x)), 
				'gsep_y': lambda x: int(np.mean(x))})

		totalrel = totalrel.reset_index()
		totalrel.columns = ['descendant_x', 'descendant_y', 'kinship_sum', 'n_shared_anc', 'gsep_x', 'gsep_y']
		del minrels
		del mrca
		del matched_ancestors

		rel_cats = pd.DataFrame({
			'gens_up': [x[0] for x in REL.keys()], 
			'gen_down': [x[1] for x in REL.keys()],
			'nshared_ancestors': [x[2] for x in REL.keys()],
			'rel': [x for x in REL.values()],
			'kinship': [x[2] * 0.5**(x[0]+x[1]+1) for x in REL.keys()],
		})

		# assign relative categories
		rels = [] 
		for row in totalrel[['gsep_x','gsep_y', 'n_shared_anc']].itertuples(index = False):
			#ensure the order matches 
			rel_tup = tuple(row) if (row[0]>=row[1]) else tuple((row[1], row[0], row[2]))
			rels.append(REL.get(rel_tup, 'undef'))

		totalrel['rel'] = rels
		del rels 
		
		# change dtypes to take less memory
		totalrel[['descendant_x','descendant_y']] = totalrel[['descendant_x','descendant_y']].astype(np.int32)
		totalrel[['n_shared_anc','gsep_x', 'gsep_y']] = totalrel[['n_shared_anc','gsep_x', 'gsep_y']].astype(np.int8)
		totalrel[['kinship_sum']] = totalrel[['kinship_sum']].astype(np.float32)

		# write data frame out to compressed hdf file
		totalrel.to_csv(self.fileOutput, sep='\t', index=None)
		mr = tracemalloc.get_traced_memory()
		print(mr[1]/1024)
		tracemalloc.stop()
