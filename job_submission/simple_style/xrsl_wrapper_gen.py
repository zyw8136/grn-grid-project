#!/usr/bin/env python

from itertools import repeat
import sys

# generate XRSL files for submission, 1.xrsl - n.xrsl

# sample:
base1 = '&("executable" = "wrapper_%d.sh")("executables" = "aracne2")("jobname" = "gpl5424_%d" )(stdout=std.out)(stderr=std.err)("inputfiles" = ("wrapper_%d.sh" "") ("aracne2" "") ("gpl5424.tsv" "") ("chunk_%d.txt" "") )(outputfiles=("gpl5424_output_%d.adj" "") ("std.err" "") ("std.out" "") ("chunk_%d.txt" ""))(memory>="900")'
base2 = '#!/bin/bash \n ./aracne2 -i gpl5424.tsv -s chunk_%d.txt -k 0.16 -t 0.05 -e 0.1 -o gpl5424_output_%d.adj'

for subnet in range(1,int(sys.argv[1])+1):
	new1 = base1 % tuple([subnet]*6)
	new2 = base2 % tuple([subnet]*2)

	fh = open(str(subnet) + '.xrsl', "w")
	fh.write(new1)
	fh.close()
        fh = open('wrapper_'+str(subnet)+'.sh', "w")
        fh.write(new2)
        fh.close()


