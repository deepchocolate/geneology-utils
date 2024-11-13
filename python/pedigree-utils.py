#!/usr/bin/env python3
import sys
from cla.components import Components
from cla.ancestors import Ancestors
from cla.relatives import Relatives

if __name__ == '__main__':
	import argparse as ap
	p = ap.ArgumentParser('Identify pedgree components')
	p.add_argument('--file-input', metavar='file', type=ap.FileType('r'), default=sys.stdin, help='Input file, if omitted read from stdin')
	p.add_argument('--file-output', metavar='file', type=ap.FileType('w'), default=sys.stdout, help='Output file, if omitted write to stdout')
	p.add_argument('--file-sep', metavar='separator', default='\t', type=str, help='Field separator in input data')
	p.add_argument('--col-individual', default='ID', type=str, help='Column name of index individual')
	p.add_argument('--col-mother', default='MOTHER', type=str, help='Column name of mother')
	p.add_argument('--col-father', default='FATHER', type=str, help='Column name of father')
	p.add_argument('--cores', type=int, help='Number of cores used for parallel processing when used. If omitted, N - 1 will be used')
	p.add_argument('--verbose', action='store_true', help='Describe what is happening.')
	sp = p.add_subparsers(required=True)
	# Components
	sp_pedigrees = sp.add_parser('identify-pedigrees', help='Identify pedigrees')
	sp_pedigrees.set_defaults(func=lambda pars : Components(pars.file_input, pars.file_output, pars.file_sep)) 
	sp_pedigrees.add_argument('--col-ind', metavar='[column]', help='Column name or index for index individual')
	sp_pedigrees.add_argument('--col-parent', metavar='[column]', help='Column name or index for parent of index individual')
	# Ancestors
	sp_anc = sp.add_parser('get-ancestors', help='Get ancestors')
	sp_anc.add_argument('--generations-max', metavar='[generations-max]', default=10, help='Max generations up/down', type=int)
	sp_anc.add_argument('--file-intermediate', type=str, default='offspring-parents.p5')
	sp_anc.add_argument('--sort', type=str, metavar='[column]', nargs='?', help='Sort output data on an output column')
	#sp_anc.add_argument('--splits', type=int, required=True, help='Number of parallel jobs')
	#sp_anc.add_argument('--split-index', type=int, required=True, help='Current index for parallel processing')
	sp_anc.set_defaults(func=Ancestors.run)
	# Relatives
	sp_rel = sp.add_parser('get-relatives', help='Get relatives')
	sp_rel.set_defaults(func=Relatives.run)
	args = p.parse_args()
	# Print help and exit if stdin is empty and no arguments have been passed
	if len(sys.argv) == 1 and sys.stdin.isatty():
		p.print_help()
		quit()
	args.func(args)
	

