#!/bin/bash
#BASENAME=pipeline
BAHN=separate_template.bahn
#DBNAME=results_db.sqlite    --- DEPRICATED
HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
KRAFTWERK=kraftwerk.py

# init database
#$HOME/$KRAFTWERK $BAHN init ./$BASENAME/ $DBNAME

# NOTE: -db optional flag for dbName
$HOME/$KRAFTWERK $BAHN init .

# insert modules
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/autobahn.py
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/yasdbf_main.py
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/pepxml.py
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/Tandem2XML_wrapper.py
# NOTE: currently has hardcoded path to msgf.jar
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/msgf_file.py
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/xtandemParser.py
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/SpectraST_wrapper.py
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/Linnaeus_wrapper.py
#$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/swissprotHotfix.py
$HOME/$KRAFTWERK $BAHN insert module $HOME/source/modules/msgf_plus_file.py

# insert paths
$HOME/$KRAFTWERK $BAHN insert path . .t.xml
$HOME/$KRAFTWERK $BAHN insert path . .pep.xml -g
$HOME/$KRAFTWERK $BAHN insert path . .tsv

# insert processes (builds core of pipeline)
$HOME/$KRAFTWERK $BAHN insert process 2 100 0 createBaseTables -i con params 
$HOME/$KRAFTWERK $BAHN insert process 4 200 2 Tandem2XML -i filename -path 1
$HOME/$KRAFTWERK $BAHN insert process 3 250 2 parse_spectra -i con lib filename -path 2
#$HOME/$KRAFTWERK $BAHN insert process 7 275 2 insertSpectraSTData -i con filename -path 2
#$HOME/$KRAFTWERK $BAHN insert process 6 300 2 appendProteinInfo -i con filename -path 1
$HOME/$KRAFTWERK $BAHN insert process 5 400 2 makeMSGFs -i con params filename -path 1
$HOME/$KRAFTWERK $BAHN insert process 9 500 2 importMSGFPlus -i con filename -path 3
$HOME/$KRAFTWERK $BAHN insert process 8 550 0 Linnaeus2YASDBF -i params
$HOME/$KRAFTWERK $BAHN insert process 2 600 0 vacuum -i con

# change to structured pipeline
$HOME/$KRAFTWERK $BAHN update params library_clustering separate
