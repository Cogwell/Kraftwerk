#!/usr/bin/python
import sqlite3, argparse, textwrap, sys
import numpy as N
import copy

"""
Attempt to create Protein_Coverage table but if already exists
stop because then it is most likely already populated
"""
def createProteinCoverage(con, cur):
    try:
        cur.execute("""CREATE TABLE Protein_Coverage
            (id INTEGER PRIMARY KEY, libID INTEGER, name VARCHAR, gi VARCHAR, 
            coverage FLOAT, proE FLOAT)""")
        con.commit()
    except sqlite3.OperationalError, ex:
        if "already exists" in str(ex):
            sys.exit("Protein_Coverage already exists.")
        else:
            raise

# sticking all code here to do try catch cleanUp for now
def main(con, cur):
    # get protein data
    cur.execute("SELECT gi, name, length FROM Protein_DB_Info")
    con.commit()
    data = cur.fetchall()
    
    gis = [str(p[0]) for p in data]
    names = [str(p[1]) for p in data]
    arys = [N.zeros(p[2],dtype=N.int8) for p in data]
   
    # count libraries
    cur.execute("SELECT count(*) FROM libraries")
    con.commit()
    libCnt = cur.fetchone()[0]
    
    # do this for each library
    values = []
    for libID in xrange(libCnt):
        # get proE values
        cur.execute("SELECT Type, FileLoc FROM libraries WHERE LibID = %i" % libID)
        con.commit()
        data = cur.fetchone()
        libType = str(data[0])
        
        if libType == "COMPILE":
            
            files = data[1].split(',')
            #libName = files[0].split('.')[0]
            files = [x.split('.')[0] + '_Filter.mgf' for x in files]
           
            curArys = copy.deepcopy(arys)

            # NOTE: REPLACE THIS IN THE FUTURE (SO UGLY)
            sql = "SELECT Protein, MIN(ProE) FROM proteins WHERE "
            for f in files:
                sql += "MSGFName = '%s' OR " % f
            sql = sql[0:-3]
            sql += "GROUP BY Protein"
            cur.execute(sql)
            con.commit()
            data = cur.fetchall()

            proNames = []
            proEvals = []
            for i in data:
                parts = str(i[0]).split('|')
                if len(parts) > 1:
                    proNames.append(parts[1])
                    proEvals.append(i[1])

            # excute statement and reterieve data
            cur.execute("SELECT DISTINCT Name, Protein, PepStart, PepStop FROM spectra WHERE LibID = %i" % libID)
            con.commit()
            data = cur.fetchall()

            for i in data:
                protList = i[1].split('/')
                protList.pop(0)
                for prot in protList:
                    parts = prot.split('|')
                    if len(parts) > 1:
                        prot = parts[1]
                        pepStart = i[2]
                        pepStop = i[3]
                        try:
                            if prot.isdigit():
                                idx = gis.index(prot)
                            else:
                                idx = names.index(prot)
                            curArys[idx][pepStart:pepStop] = 1
                        except:
                            continue
            
            for i, prot in enumerate(names):
                coverage = curArys[i].sum() / float(len(curArys[i]))
                if coverage > 0:
                    proE = 0
                    try:
                        idx = proNames.index(prot)
                        proE = proEvals[idx]
                    except:
                        pass
                    try:
                        idx = proNames.index(gis[i])
                        proE = proEvals[idx]
                    except:
                        pass
                    values.append( (libID, prot, gis[i], coverage, proE) )
            
        
    cur.executemany("""INSERT INTO Protein_Coverage
        ('libID', 'name', 'gi', 'coverage', 'ProE')
        VALUES (?,?,?,?,?)""", values)
    con.commit()

def cleanUp(con, cur):
    # mainly for deleting Protein_DB_INFO table
    cur.execute("DROP TABLE Protein_Coverage")
    con.commit()

if __name__ == "__main__":
    # create parser
    desc = '''\
           proCover calculates protein coverage of a yasdbf'''
    p = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                description=textwrap.dedent(desc))
    p.add_argument('db', type=str, help='Path to yasdbf file')
    args = p.parse_args()

    # connect to database
    con = sqlite3.connect(args.db)
    cur = con.cursor()
        
    createProteinCoverage(con, cur)
   
    try:
        main(con, cur)
    except:
        cleanUp(con, cur)
