import os, sys
import yasdbf_main

def Linnaeus2YASDBF(params):
    db_name = params['DB Name']
    linnaeus = '/home/d3y034/chembio_svn/Proteomics/Linnaeus/Linnaeus.py'
    if os.path.exists(linnaeus):
        yasdbf_main.runCmd( "%s %s %s" % (linnaeus, 'compile2YASDBF', db_name) )
    else:
        sys.exit('Linnaeus is not in the specifed location.')
