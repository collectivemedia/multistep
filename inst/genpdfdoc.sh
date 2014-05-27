#!/bin/bash

# go to the root directory of the package
cd ..

vers=`grep Version: DESCRIPTION | awk '{print $2}'`

R --vanilla --quiet -e 'library(devtools); document()'

export R_RD4PDF=times,hyper
rm -f inst/multistep_${vers}.pdf 
R CMD Rd2pdf -o inst/multistep_${vers}.pdf .

