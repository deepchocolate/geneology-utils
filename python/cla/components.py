# This script is based on github.com/yorugosu/genealogy/blob/main/components.py
import networkx as nx
import pandas as pd

class Components:
	
	fileSep = None
	fileInput = None
	fileOutput = None
	
	def __init__(self, fileInput, fileOutput, fileSep):
		self.fileInput = fileInput
		self.fileOutput = fileOutput
		self.fileSep = fileSep
		comps = self.get()
		self.write(comps)
	
	def getOffspringParent(self):
		dta = pd.read_csv(self.fileInput, sep='\t')
		out1 = dta['ID'].astype(str) + ' ' + dta['MOTHER'].astype(str)
		out2 = dta['ID'].astype(str) + ' ' + dta['FATHER'].astype(str)
		out = pd.concat([out1, out2])
		return out.to_numpy()
	
	def get(self):
		pairs = self.getOffspringParent()
		G = nx.parse_edgelist(pairs, nodetype=int, data=(("weight", float),))
		#find the components of the graph
		components = nx.connected_components(G)
		#register id's in order of appearance in the components
		indid = []
		
		#register component size
		sizes = []
		for component in components:
			indid.extend(component)
			sizes.append(len(component))
		# register famid's according to component size
		famid = []

		for i in range(len(sizes)):
			famid.extend([i+1]*sizes[i])
		return pd.DataFrame({'famid':famid, 'indid':indid})
	
	def write(self, dfPD):
		export_csv = dfPD.to_csv(self.fileOutput, sep=',', index=None, header=True)
