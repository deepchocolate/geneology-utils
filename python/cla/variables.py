# x is the number of generations up
# y is the number of generations down
# z is the number of inheritance paths
# assume x is >= y

REL = {
	# direct ancestors
	(1, 0, 1): 'PO',
	(2, 0, 1): '1G',
	(3, 0, 1): '2G',
	(4, 0, 1): '3G',
	(5, 0, 1): '4G',
	# siblings
	(1, 1, 2): 'FS', # Full sibling
	(1, 1, 1): 'HS', # Half sibling
	# avuncular
	(2, 1, 2): 'Av', # Eg uncle - newphew
	(2, 1, 1): 'HAv',
	(3, 1, 2): '1GAv',
	(3, 1, 1): '1GHAv',
	(4, 1, 2): '2GAv',
	(4, 1, 1): '2GHAv',
	(5, 1, 2): '3GAv',
	(5, 1, 1): '3GHAv',
	# 1st cousins
	(2, 2, 2): '1C', # also twice first half cousins?
	(2, 2, 1): 'H1C',
	(3, 2, 2): '1C1R',
	(3, 2, 1): 'H1C1R',
	(4, 2, 2): '1C2R',
	(4, 2, 1): 'H1C2R',
	(5, 2, 2): '1C3R',
	(5, 2, 1): 'H1C3R',    
	# 2nd cousins 
	(3, 3, 2): '2C',
	(3, 3, 1): 'H2C',
	(4, 3, 2): '2C1R',
	(4, 3, 1): 'H2C1R',
	(5, 3, 2): '2C2R',
	(5, 3, 1): 'H2C2R',   
	# 3rd cousins
	(4, 4, 2): '3C',
	(4, 4, 1): 'H3C',
	(5, 4, 2): '3C1R',
	(5, 4, 1): 'H3C1R',
	# 4th cousins
	(5, 5, 2): '4C',
	(5, 5, 1): 'H4C',
			
	# extended relatives
	(2, 2, 4): '2x1C',
	(2, 2, 3): '1.5x1C',
	(3, 2, 4): '2x1C1R',
	(3, 2, 3): '1.5x1C1R',
	(4, 2, 4): '2x1C2R',
	(4, 2, 3): '1.5x1C2R',
    
	(3, 3, 4): '2x2C',
	(3, 3, 3): '1.5x2C',
	(4, 3, 4): '2x2C1R',
	(4, 3, 3): '1.5x2C1R',
}
