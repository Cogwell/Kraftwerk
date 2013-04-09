import sys, os, sqlite3, argparse, textwrap, time, traceback
from subprocess import Popen, PIPE, STDOUT
# Borrowed from Brian Clowers with small edits
# runs the cmd and prints out errors in the form of traceback
# NOTE: TODO: this will not print out errors that are returned by a command

def runCmd(cmd):
    print cmd
    try:
        p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        stdout, stderr = p.communicate()
        print stdout
    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        traceback.print_exception(exceptionType, exceptionValue, exceptionTraceback, file=sys.stdout)
        print "Sorry: %s\n\n:%s\n%s\n" % (exceptionType, exceptionValue, exceptionTraceback)

def createBaseTables(con, params):
    """ Create base tables """
    if os.path.getsize(params['DB Name']) is not 0:
        sys.exit("Database already exists. Please move or delete sqlite file.")

    con.execute("""CREATE TABLE files
        (id INTEGER PRIMARY KEY, library_id INTEGER, basename VARCHAR NOT NULL, dirname VARCHAR, 
        engine_id INTEGER, FOREIGN KEY(engine_id) REFERENCES search_engines(id))""")
    
    con.execute("""CREATE TABLE spectra 
        (id INTEGER PRIMARY KEY, file_id VARCHAR, name VARCHAR UNIQUE, scan_num INTEGER, peptide VARCHAR, 
        peptide_modded VARCHAR, neutral_mass FLOAT, charge INTEGER, retention_time FLOAT, 
        n_terminal CHAR(1), c_terminal CHAR(1), protein VARCHAR, total_ions INTEGER, 
        matched_ions INTEGER, ppm_error FLOAT, FOREIGN KEY(file_id) REFERENCES files(id) )""")
    
    con.execute("""CREATE TABLE homology
        (peptide VARCHAR, protein VARCHAR, CONSTRAINT uc_pepprot UNIQUE(peptide, protein))""")
    
    con.execute("""CREATE TABLE search_engines
        (id INTEGER PRIMARY KEY, search_engine VARCHAR, pep_tbl_name VARCHAR, pro_tbl_name VARCHAR)""")
    con.commit()
    
    search_engines = [("X! Tandem", "tandem_peptides", "tandem_proteins"), 
                      ("X! Tandem (k-score)", "tandem_peptides", "tandem_proteins")]
    con.executemany("""INSERT INTO search_engines ('search_engine', 'pep_tbl_name', 'pro_tbl_name') 
        VALUES (?,?,?)""", search_engines)
    
    con.execute("""CREATE TABLE tandem_peptides
        (id INTEGER PRIMARY KEY, nextscore FLOAT, pepE FLOAT,
        FOREIGN KEY(id) REFERENCES spectra(id) )""")
    con.execute("""CREATE TABLE tandem_proteins
        (id INTEGER, protein VARCHAR, proE FLOAT, pepStart INTEGER, pepStop INTEGER,
        FOREIGN KEY(id) REFERENCES spectra(id) )""")
    con.commit()
    
    #NOTE: for performance this should occur at the end of building a database
    createIndices(con)

def createIndices(con):
    con.execute("CREATE INDEX idx_files_library_id ON files (library_id)")
    con.execute("CREATE INDEX idx_spec_file_id ON spectra (file_id)")
    con.execute("CREATE INDEX idx_spec_name ON spectra (name)")
    con.execute("CREATE INDEX idx_spec_ppm_error ON spectra (ppm_error)")
    con.execute("CREATE INDEX idx_spec_protein ON spectra (protein)")
    con.execute("CREATE INDEX idx_se_search_engine ON search_engines (search_engine)")
    con.execute("CREATE INDEX idx_hmlg_peptide ON homology (peptide)")
    con.execute("CREATE INDEX idx_hmlg_protein ON homology (protein)")
    con.execute("CREATE INDEX idx_tp_id ON tandem_proteins (id)")
    con.execute("CREATE INDEX idx_tp_protein ON tandem_proteins (protein)")
    con.commit()

def insertSpectraDict(con, spectraDict):
    spectra_values = []
    tandem_peps = []

    # get next spectra_id if it exists, otherwise specra_id = 1
    spectra_id = con.execute("SELECT MAX(id) FROM spectra").fetchone()[0]
    if not spectra_id:
        spectra_id = 1
    else:
        spectra_id +=1

    # build fileDict to connect spectra back to files table
    fileDict = {}
    for rec in con.execute("SELECT id, basename, engine_id FROM files"):
        key = str(rec['basename']).lower().split('.')[0]
        fileDict[key] = ( int(rec['id']), int(rec['engine_id']) )

    # get modification dict
    modsDict = spectraDict['mods']
    del spectraDict['mods']

    for specName in spectraDict.keys():
        # NOTE: this assumes every bit of data is populated... that seems bad...
        file_id, engine_id = fileDict[ specName.lower().split('.')[0] ]
        cur_spec = spectraDict[specName]
        hit = cur_spec['search_hit'][0]
        pep = hit['peptide']
        
        # caclulate modified peptide sequence
        mods = [ "" for aa in pep ] # init mods array
        flag = False
        for pos, mass in cur_spec['mods'][0].iteritems():
            try:
                # i believe pos starts at 1
                pos = pos-1 # adjust for counting differences
                if modsDict[mass] > 0.0:
                    mods[pos] = "+" + str(modsDict[mass])
                else:
                    mods[pos] = str(modsDict[mass])
                flag = True
            except:
                flag = False
                break
        pepModded = ""
        if flag:
            for i, aa in enumerate(pep):
                pepModded = pepModded + aa + mods[i] 
                
        mass = cur_spec['neutral_mass']
        massdiff = hit['massdiff']
        ppmErr = massdiff / mass * 1e6
        
        # handle scoring first
        if engine_id == 1 or engine_id == 2:
            # create tandem score entry
            tandem_peps.append( (spectra_id, hit['nextscore'], hit['expect']) )
        
        # build list of proteins for this peptide
        primary_pro = stripUniprot(hit['protein'])
        try:
            con.execute("INSERT INTO homology VALUES (?,?)", (pep, primary_pro))
        except:
            pass
        if 'alternative_protein' in hit:
            for p in hit['alternative_protein']:
                try:
                    con.execute("INSERT INTO homology VALUES (?,?)", (pep, stripUniprot(p)))
                except:
                    pass
        
        # AARON: 1/27/12 - this is a hack for trailing spaces on spectra names
        specName = specName.strip()
        # AARON: 3/13/13
        specName = specName.split('.')
        scanNum = repr(int(specName[1]))
        specName = "%s.%s.%s.%s" % (specName[0], scanNum, scanNum, specName[3])

        # create spectra entry
        spectra_values.append( (spectra_id, file_id, specName, scanNum, pep, pepModded, mass, cur_spec['charge'], cur_spec['retention_time'], 
            hit['nterminal'], hit['cterminal'], primary_pro, hit['total_ions'], hit['matched_ions'], ppmErr) )

        spectra_id +=1
    
    con.executemany("""INSERT INTO spectra 
        (id, file_id, name, scan_num, peptide, peptide_modded, neutral_mass, charge, retention_time, n_terminal, 
        c_terminal, protein, total_ions, matched_ions, ppm_error) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", spectra_values)
    con.executemany("INSERT INTO tandem_peptides VALUES (?,?,?)", tandem_peps)

    con.commit()

def stripUniprot(prot):
    """ strips uniprot id if it is available """
    #weird assumption -> if you ever see a 6 len str after removing pipes... you have it!
    for val in str(prot).split('|'):
        if len(val.split('-')[0]) == 6:
            return val
    return prot

def insertTandemProInfo(con, tandemProInfo):
    cur = con.cursor()
    tandem_pros = []

    for specName, protList in tandemProInfo.iteritems():
        #print "'%s'" % specName
        #AARON 020512 - looks like .dta in way
        specName = specName.replace('.dta','')
        
        cur.execute("SELECT id FROM spectra WHERE name =?", (specName,) )
        
        spectra_id = cur.fetchone()['id']
        
        for proInfo in protList:
            tandem_pros.append( (spectra_id, stripUniprot(proInfo[0]), float(proInfo[1]), int(proInfo[2]), int(proInfo[3])) )
    
    con.executemany("INSERT INTO tandem_proteins VALUES (?,?,?,?,?)", tandem_pros)

# sometimes you just need to do some spring cleaning
def vacuum(con):
    con.execute("VACUUM")
    con.commit()
