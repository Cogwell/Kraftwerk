import os, sys, sqlite3
import yasdbf_main

# NOTE: consider standardizing something like this...
def alter_table(con):
    con.execute("ALTER TABLE spectra ADD x VARCHAR")
    con.execute("ALTER TABLE spectra ADD y VARCHAR")
    con.execute("ALTER TABLE spectra ADD labels VARCHAR")
    con.execute("ALTER TABLE spectra ADD precur_int FLOAT")
    con.execute("ALTER TABLE spectra ADD total_int FLOAT")
    con.execute("ALTER TABLE spectra ADD matched_int FLOAT")

def insertSpectraSTData(con, pepXML):
    # NOTE: consider avoiding hardcoding this
    spectraST = "/usr/bin/spectrast"
    if os.path.exists(spectraST):
        # keep relative path in base
        path, base = os.path.split(pepXML)
        base = base.split('.')[0]
        yasdbf_main.runCmd( "%s -c -cP0 %s" % (spectraST, pepXML) )
        
        sptxt = os.path.join(path, base + '.sptxt')
        specST_dict = parseSPTXT(sptxt)
       
        # NOTE: try to alter table 
        # TODO: remove!
        try:
            alter_table(con)
        except sqlite3.OperationalError, ex:
            if "duplicate column name" in str(ex):
                pass
            else:
                raise

        data = []
        for k, v in specST_dict.iteritems():
            data.append( (';'.join(v[0]), ';'.join(v[1]) , ';'.join(v[2]), v[3], v[4], v[5], k) )
       
        con.executemany("UPDATE spectra SET x=?, y=?, labels=?, precur_int=?, total_int=?, matched_int=? WHERE name=?", data) 
        con.commit()
    else:
        sys.exit("SpectraST does not appear to be installed")
    
def parseSPTXT(sptxt):
    specST_dict = {}
    with open(sptxt, 'r') as f:
        line = f.readline()
        # parse out comment lines
        while line.startswith("###"):
            line = f.readline()

        commentDict = {}
        while line:
            name = line.split()[1]
            libID = f.readline().split()[1]
            mw = f.readline().split()[1]
            precursorMZ = f.readline().split()[1]
            status = f.readline().split()[1]
            fullname = f.readline().split()[1]
            # comments (split off header, strip qutoes, and split on spaces) 
            comments = f.readline().split(' ',1)[1].replace("\"", "").split()
            for c in comments:
                try:
                    temp = c.split('=')
                    commentDict[temp[0]] = temp[1]
                except:
                    print comments
                    print temp
                    raise
            numpeaks = f.readline().split()[1]

            x = []
            y = []
            labels = []
            totalInt = 0.0
            matchedInt = 0.0
            line = f.readline()
            while len(line) > 1:
                temp = line.split()
                x.append(temp[0])
                y.append(temp[1])
                labels.append(temp[2])
                line = f.readline()
                curInt = float(temp[1])
                totalInt = totalInt + curInt
                if temp[2] != "?":
                    matchedInt = matchedInt + curInt
            
            # NOTE: populate dictionary with desire values
            precurInt = float(commentDict['PrecursorIntensity'])
            specST_dict[commentDict['RawSpectrum']] = (x, y, labels, precurInt, totalInt, matchedInt)

            # get nextline
            line = f.readline()
    
    return specST_dict
