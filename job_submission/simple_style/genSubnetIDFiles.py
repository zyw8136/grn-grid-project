#!/usr/bin/env python

# generate subnetwork-ID files
import sys

counter = 0 # count genes added to output
filename = 1 # filenames will be just numbers
buffer = ""

for gene in range(1, int(sys.argv[1])+1, 1):
	buffer += 'G'+str(gene) + "\n"
	counter += 1

	if (counter % 5 == 0 or gene == int(sys.argv[1])):
		fh = open('chunk_' + str(filename) + '.txt', "w")
		fh.write(buffer)
		fh.close()
		filename += 1
		buffer = ""
