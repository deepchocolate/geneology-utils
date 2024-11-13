import unittest
import subprocess
import os.path
import pandas as pd
import numpy as np
import numpy.testing as npt
import hashlib

class TestTestData(unittest.TestCase):
	
	ids = None
	
	def test_testDataCreation(self):
		out = subprocess.run(['Rscript', 'create-test-data.R'])
		self.assertEqual(0, out.returncode)
		#print(out)
		fileTrios = 'input/trios.tsv'
		self.assertTrue(os.path.isfile(fileTrios))
		dtaTrios = pd.read_csv(fileTrios, sep='\t')
		# Will change for something flexible
		np.testing.assert_array_equal(['ID', 'FATHER', 'MOTHER', 'SEX', 'FAMID'], dtaTrios.columns.values)
		np.testing.assert_array_equal([13,23,33,105,106,53,104], dtaTrios['ID'])
		np.testing.assert_array_equal([11,21,31,104,104,51,51], dtaTrios['FATHER'])
		np.testing.assert_array_equal([12,22,32,91,91,52,52], dtaTrios['MOTHER'])
		np.testing.assert_array_equal([1,1,1,1,1,1,1], dtaTrios['SEX'])
	
	def getParentOffspring(self, f):
		dtaTrios = pd.read_csv(f, sep='\t')
		p = pd.DataFrame({'ID':dtaTrios['ID'], 'PARENT':dtaTrios['MOTHER']})
		p2 = pd.DataFrame({'ID':dtaTrios['ID'], 'PARENT':dtaTrios['FATHER']})
		p = pd.concat([p, p2])
		return p

	def test_generatePedComponents(self):
		fileTrios = 'input/trios.tsv'
		#dtaTrios = pd.read_csv(fileTrios, sep='\t')
		po = self.getParentOffspring(fileTrios)
		ids = pd.concat([po['ID'], po['PARENT']])
		ids = pd.unique(ids)
		# Run components
		fileComponents = 'output/components.csv'
		out = subprocess.run(['python3', '../python/pedigree-utils.py', '--file-input', fileTrios, '--file-output', fileComponents, 'identify-pedigrees'])
		self.assertEqual(0, out.returncode)
		dtaPID = pd.read_csv(fileComponents, sep=',')
		npt.assert_array_equal(ids.sort(), dtaPID['indid'].to_numpy().sort())
		# 4 pedigrees
		self.assertEqual(4, len(pd.unique(dtaPID['famid'])))
		

	def test_ancestors(self):
		#po = self.getParentOffspring('intput/trios.tsv')
		filePO = 'output/population.tsv'
		fileRes = 'output/offspring-ancestor.tsv'
		#po.to_csv(filePO, sep=',', index=None, header=True)
		out = subprocess.run(['python3', '../python/pedigree-utils.py', '--file-input', filePO, '--file-output', fileRes, 'get-ancestors', '--file-intermediate', 'output/offspring-parents.p5', '--sort', 'child ancestor'])
		self.assertEqual(0, out.returncode)
		pop = pd.read_csv(fileRes, sep='\t')
		self.assertEqual(18, len(pop['child']))
		with open(fileRes, 'rb') as f:
			md5 = hashlib.md5(f.read()).hexdigest()
		self.assertEqual('918401ab6230f89dd92b71f550021328', md5)
	
	def test_relatives(self):
		fileOA = 'output/offspring-ancestor.tsv'
		fileRes = 'output/relatives.tsv'
		out = subprocess.run(['python3', '../python/pedigree-utils.py', '--file-input', fileOA, '--file-output', fileRes, 'get-relatives'])
		with open(fileRes, 'rb') as f:
			md5 = hashlib.md5(f.read()).hexdigest()
		self.assertEqual('81b1f82a7b4e187d1b0420187c892f3f', md5)

if __name__ == '__main__':
	unittest.main()

