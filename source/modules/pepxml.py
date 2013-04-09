import xml.sax, time, os, sqlite3
import yasdbf_main

# http://code.google.com/p/massspec-toolbox/source/browse/trunk/search/pepxml.py?spec=svn51&r=51
# massspec-toolbox pepxml.py

class pepxml_parser(xml.sax.ContentHandler):
    
    def __init__(self):
        self.element_array = []
        self.is_spectrum_query = False
        self.is_search_hit = False
        self.PSM = dict()
        # keep track of all modifications possible in this file
        self.PSM['mods'] = {}
        # keep track of mods on a spectra
        self.modifications = dict()
        self.search_hit = dict()
        self.spectrum_id = ''

    def startElement(self,name,attr):
        self.element_array.append(name)
        if( len(self.element_array) == 2 and name == 'msms_run_summary' ):
            self.PSM['data_file'] = "%s%s" % ( str(attr['base_name']), str(attr['raw_data']) ) 
        elif( len(self.element_array) == 3 ):
            if( name == 'spectrum_query' ):
                self.is_spectrum_query = True
                self.spectrum_id = str(attr['spectrum'])
                if( not self.PSM.has_key(self.spectrum_id) ):
                    self.PSM[self.spectrum_id] = dict()
                    self.PSM[self.spectrum_id]['mods'] = []
                    self.PSM[self.spectrum_id]['search_hit'] = []
                else:
                    print "Duplicate PSM : %s"%self.spectrum_id
                self.PSM[self.spectrum_id]['charge'] = int(attr['assumed_charge'])
                self.PSM[self.spectrum_id]['neutral_mass'] = float(attr['precursor_neutral_mass'])
                self.PSM[self.spectrum_id]['retention_time'] = float(attr['retention_time_sec'])
            elif( name == 'search_summary' ):
                self.PSM['search_engine'] = str(attr['search_engine'])
        elif( len(self.element_array) == 4 and name == 'aminoacid_modification' ):
            self.PSM['mods'][ float(attr['mass']) ] = float(attr['massdiff'])
            #terminal_modification
        elif( len(self.element_array) == 5 and name == 'search_hit' ):
            self.is_search_hit = True
            self.search_hit = dict()
            self.search_hit = dict()
            self.search_hit['hit_rank'] = int(attr['hit_rank'])
            self.search_hit['peptide'] = str(attr['peptide'])
            self.search_hit['protein'] = str(attr['protein'])
            self.search_hit['missed_cleavages'] = int(attr['num_missed_cleavages'])
            self.search_hit['nterminal'] = str(attr['peptide_prev_aa'])
            self.search_hit['cterminal'] = str(attr['peptide_next_aa'])
            self.search_hit['matched_ions'] = int(attr['num_matched_ions'])
            self.search_hit['total_ions'] = int(attr['tot_num_ions'])
            self.search_hit['massdiff'] = float(attr['massdiff'])
        elif( len(self.element_array) == 6 ):
            if( name == 'alternative_protein'):
                if( not self.search_hit.has_key('alternative_protein') ):
                    self.search_hit['alternative_protein'] = []
                self.search_hit['alternative_protein'].append(str(attr['protein']))
            elif( name == 'search_score'):
                ## SEQUEST
                if(attr['name'] == 'xcorr'):
                    self.search_hit['xcorr'] = float(attr['value'])
                elif(attr['name'] == 'spscore'):
                    self.search_hit['spscore'] = float(attr['value'])
                elif(attr['name'] == 'deltacn'):
                    self.search_hit['deltacn'] = float(attr['value'])
                ## X!Tandem
                elif(attr['name'] == 'hyperscore'):
                    self.search_hit['hyperscore'] = float(attr['value'])
                elif(attr['name'] == 'expect'):
                    self.search_hit['expect'] = float(attr['value'])
                elif(attr['name'] == 'nextscore'):
                    self.search_hit['nextscore'] = float(attr['value'])
                ## InsPecT
                elif(attr['name'] == 'mqscore'):
                    self.search_hit['mqscore'] = float(attr['value'])
                elif(attr['name'] == 'fscore'):
                    self.search_hit['fscore'] = float(attr['value'])
                elif(attr['name'] == 'deltascore'):
                    self.search_hit['deltascore'] = float(attr['value'])
                ## MyriMatch
                elif(attr['name'] == 'mvh'):
                    self.search_hit['mvh'] = float(attr['value'])
                elif(attr['name'] == 'massError'):
                    self.search_hit['massError'] = float(attr['value'])
                elif(attr['name'] == 'mzSSE'):
                    self.search_hit['mzSSE'] = float(attr['value'])
                elif(attr['name'] == 'mzFidelity'):
                    self.search_hit['mzFidelity'] = float(attr['value'])
                elif(attr['name'] == 'newMZFidelity'):
                    self.search_hit['newMZFidelity'] = float(attr['value'])
                elif(attr['name'] == 'mzMAE'):
                    self.search_hit['mzMAE'] = float(attr['value'])
                ## DirecTag-TagRecon
                elif(attr['name'] == 'numPTMs'):
                    self.search_hit['numPTMs'] = int(attr['value'])
            #elif( name == 'modification_info'):
            #     {pos:mass}
            #    self.modifications = {}
        elif( len(self.element_array) == 7 ):
            if ( name == 'mod_aminoacid_mass' ):
                self.modifications[ int(attr['position']) ] = float(attr['mass'])
            ## PeptideProphet
            elif( name == 'peptideprophet_result' ):
                self.search_hit['TPP_pep_prob'] = float(attr['probability'])

    def endElement(self,name):
        if( len(self.element_array) == 3 and name == 'spectrum_query' ):
            self.spectrum_id = ''
            self.is_spectrum_query = False
        elif( len(self.element_array) == 5 and name == 'search_hit' ):
            self.PSM[self.spectrum_id]['mods'].append(self.modifications)
            self.PSM[self.spectrum_id]['search_hit'].append(self.search_hit)
            self.modifications = dict()
            self.search_hit = dict()
            self.is_search_hit = False
        self.element_array.pop()
    
def parse_by_filename(filename_pepxml):
    p = pepxml_parser()
    xml.sax.parse(filename_pepxml,p)
    return p.PSM

def parse_spectra(con, lib, filename):
    con.row_factory = sqlite3.Row # makes query results work simular to dict type
    spectraDict = parse_by_filename(filename) # parse pepxml file
    # get spectra specific keys from Dict
    dirname, basename = os.path.split( spectraDict.pop('data_file') )
    search_engine = spectraDict.pop('search_engine')
    
    cur = con.cursor()
    cur.execute("SELECT id FROM search_engines WHERE search_engine = ?", (search_engine,) )
    engine_id = cur.fetchone()['id'] 
    data = (lib, basename, dirname, engine_id)
    con.execute("""INSERT INTO files
        ('library_id', 'basename', 'dirname', 'engine_id')
        VALUES (?,?,?,?)""", data)
    con.commit()
    # do we want this function in yasdbf_main.py
    yasdbf_main.insertSpectraDict(con, spectraDict)
