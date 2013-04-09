import sys, os, sqlite3, argparse, textwrap
import yasdbf_main

# NOTE: this whole thing needs a lot of reworking

def makeMSGFs(con, params, filename):
    # add msgf column to spectra table
    try:
        con.execute("ALTER TABLE spectra ADD msgf_score FLOAT")
        # if column exists then so does index too?
        con.execute("CREATE INDEX idx_msgf_score ON spectra (msgf_score)")
    except sqlite3.OperationalError, ex:
        if "duplicate column name" in str(ex):
            #NOTE: maybe log some kind of error here
            pass
        else:
            raise

    enzyme = params['Enzyme']
    filename = filename.split('.')[0] + '.mzXML'
    filename = os.path.basename(filename) # make sure you have basename (important for structured mode)
    
    file_id, dirname = con.execute("SELECT id, dirname FROM files WHERE basename=?", (filename,) ).fetchone()
    con.row_factory = sqlite3.Row

    msgfPre = writePrefile(con, file_id, filename)
    msgf = runMSGF(msgfPre, dirname, enzyme)
    yasdbf_main.runCmd("rm %s" % msgfPre) # clean-up
    importMSGF(con, msgf)

def writePrefile(con, file_id, basename):
    # use basename to find name for msgfPre file
    basename = basename.split('.')[0] + '.mzXML'
    filename = basename.split('.')[0] + '.msgfPre'

    # write msgfPre file
    with open(filename, 'w') as f:
        # write headers
        f.write("#SpectrumFile\tScanName\tScan#\tAnnotation\tCharge\n")
        
        select = """SELECT name, n_terminal, peptide, peptide_modded, c_terminal, charge 
                    FROM spectra WHERE file_id = ? ORDER BY name"""
        for record in con.execute(select, (file_id,)):
            n_terminal = record['n_terminal'].replace('-','*')
            c_terminal = record['c_terminal'].replace('-','*')
            if record['peptide_modded'] != "":
                pep = record['peptide_modded']
            else:
                pep = record['peptide']
            peptide = "%s.%s.%s" % (n_terminal, pep, c_terminal)
            charge = record['charge']
            name = record['name']
            num = str(name).split('.')[1]
            writeStr = "%s\t%s\t%s\t%s\t%s\n" % (basename, name, num, peptide, charge)
            f.write(writeStr)

    return filename

def runMSGF(msgfPre, dirname, enzyme):
    if os.path.exists(msgfPre):
        msgfPath = "/home/yasdbf/apps/msgf.jar"
        msgf = os.path.join(dirname, msgfPre[:-3]) #make this a path
        if len(dirname) is 0:
            dirname = "."
        params = (msgfPath, msgfPre, dirname, msgf, enzyme)
        cmdStr = "java -Xmx2000M -jar %s -i %s -d %s -o %s -fixMod 0 -e %s" % params
        # "-fixMod 0" sets default mods to none
        # execute command
        #yasdbf_main.runCmd(cmdStr)
        return msgf
    #else
    return None

def importMSGF(con, msgf):
    with open(msgf, 'r') as f:
        keys = {}
        for i, key in enumerate(f.readline().split()):
            keys[key] = i
        
        data = []
        for line in f:
            line = line.split('\t')
            prob = line[keys['SpecProb']]
            if prob == "N/A:":
                prob = 0
            ScanName, junk = line[keys['Title']].split('.', 1)
            junk = junk.split(' ')
            ScanName = "%s.%s.%s.%s" % (ScanName, junk[2], junk[2], junk[4][0]) 
            data.append( (prob , ScanName) )

        con.executemany("UPDATE spectra SET msgf_score=? WHERE name=?", data)
        con.commit()
