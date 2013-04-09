import os, sys
import yasdbf_main

def Tandem2XML(tandemXML):
    Tandem2XML = "/home/d3y034/chembio_svn/Proteomics/Kraftwerk/apps/Tandem2XML"
    if os.path.exists(Tandem2XML):
        d = os.path.dirname(tandemXML)
        base = os.path.basename(tandemXML).split('.')[0]
        # NOTE: need to copy tandem file to new name for it to relate to mzXML
        newTandemXML = os.path.join(d, base + '.xml') 
        yasdbf_main.runCmd( "cp %s %s" % (tandemXML, newTandemXML) )
        # NOTE: this means pepXMLs follow naming of pepXMLs
        pepXML = os.path.join(d, base + '.pep.xml') 
        yasdbf_main.runCmd( "%s %s %s" % (Tandem2XML, newTandemXML, pepXML) )
        # NOTE: remove copy
        yasdbf_main.runCmd( "rm %s" % newTandemXML )
        return pepXML
    else:
        sys.exit("Tandem2XML does not appear to be installed")
