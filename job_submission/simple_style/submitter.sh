#!/bin/bash
#1200
#arc.imbg.org.ua
#cluster.immsp.kiev.ua
#ds4.ilt.kharkov.ua
#arc.univ.kiev.ua
#golowood.mao.kiev.ua
#grid.isma.kharkov.ua

for (( c=$1; c<=$2; c++ ))
  do
    arcsub -f $c.xrsl -o submitted.job -c grid.isma.kharkov.ua
    sleep 15
done
