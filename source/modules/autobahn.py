#!/usr/bin/python
import os, sys, sqlite3, glob
import argparse, textwrap
#from source import yasdbf_main
import yaml as Y

# Borrowed from Brian Clowers -- used to load yaml configuration file
def loadConfigFile(fileName):
    try:
        with open(fileName, 'r') as f:
            settings = Y.load(f)
        return settings
    except:
        print "error loading YAML Configuration File"
        return None

def clear_loop(loop_con):
    try:
        loop_con.execute("DROP TABLE loop")
        loop_con.commit()
    except:
        pass # this should be fixed to specific error

def build_loop(loop_con, clustering):
    # build loop table to hold processes for autobahn
    loop_con.execute("CREATE TABLE loop (lib VARCHAR, filename VARCHAR, module VARCHAR, function VARCHAR, inputs VARCHAR)")
    
    # build file_path locations
    # NOTE: currently doesn't use dir!!
    file_paths = []
    for rec in loop_con.execute("SELECT DISTINCT dir, ext FROM processes WHERE generated = 0"):
        file_paths.append( rec['ext'] )
    
    # used for building loops
    process_loop = []
    # TODO: NOTE: this could be problematic when levels 1 and 2 used...
    for rec in loop_con.execute("SELECT name, function, inputs, dir, ext FROM processes WHERE level > 0"):
        process_loop.append( (rec['name'], rec['function'], rec['inputs'], rec['ext']) )
    
    data = [] # this will contain the main loop instructions

    # build pre-loop section
    select = """SELECT name, function, inputs FROM processes WHERE level = 0 
                AND rowid < (SELECT MIN(rowid) FROM processes WHERE level > 0)"""
    for rec in loop_con.execute(select):
        data.append( (None, None, rec['name'].split('.')[0], rec['function'], rec['inputs']) )

    # build main looping section
    if clustering == 'structured':
        # TODO: fix dir here...
        dirs = [name for name in os.listdir('.')
                if os.path.isdir(name)]

        for i, d in enumerate(sorted(dirs)):
            # NOTE: this file is the required filetype
            basenames = set([n.split('.')[0] for n in glob.glob( os.path.join(d,'*.t.xml') )])
            for name in basenames:
                for proc in process_loop:
                    filename = name + proc[3]
                    data.append( (i+1, filename, proc[0].split('.')[0], proc[1], proc[2]) )
    else:
        basenames = set([n.split('.')[0] for n in glob.glob('*.t.xml')])
        
        if clustering == 'single':
            for name in basenames:
                for proc in process_loop:
                    data.append( (1, name+proc[3], proc[0].split('.')[0], proc[1], proc[2]) )
        elif clustering == 'separate':
            for i, name in enumerate(basenames):
                for proc in process_loop:
                    data.append( (i+1, name+proc[3], proc[0].split('.')[0], proc[1], proc[2]) )
        #else error
    # end build main looping section
    
    # build post-loop section
    select = """SELECT name, function, inputs FROM processes WHERE level = 0 
                AND rowid > (SELECT MAX(rowid) FROM processes WHERE level > 0)"""
    for rec in loop_con.execute(select):
        data.append( (None, None, rec['name'].split('.')[0], rec['function'], rec['inputs']) )

    loop_con.executemany("INSERT INTO loop VALUES (?,?,?,?,?)", data)
    loop_con.commit()

# NOTE: con and params
def main(loop_con, con, params):

    # Try to build the loop table but continue on if already exists
    try:
        build_loop( loop_con, params['Library Clustering'] ) # structured, single, or seperate
    except sqlite3.OperationalError, ex:
        if "already exists" not in str(ex):
            raise # raise error if some other issue then table already existing

    # import modules based on the modules needed for this bahn
    for rec in loop_con.execute("SELECT DISTINCT module FROM loop"):
        path = 'source.' + rec['module']
        vars()[ rec['module'] ] = __import__(path, globals(), locals(), [ rec['module'] ], -1)
    
    # This is essentially the main() of a bahn (by this point con, params need to be ready since they are possible inputs)
    for rec in loop_con.execute("SELECT rowid, lib, filename, module, function, inputs FROM loop"):
        try:
            lib = rec['lib']
            filename = rec['filename']
            func = getattr( eval(rec['module']) , rec['function'])
            funcStr = "func(%s)" % rec['inputs']
            eval(funcStr)
            loop_con.execute("DELETE FROM loop WHERE rowid=?", (rec['rowid'],) )
        except:
            loop_con.commit()
            raise
    loop_con.execute("DROP TABLE loop")
    loop_con.commit()

if __name__ == "__main__":

    desc = '''\
           This script executes the loop instructions for the data pipeline. 
           By using the autobahn.proc sqlite database to store loop execution, 
           modules are allowed to fail without losing the results of previously 
           computed modules. The autobahn.proc file is created from the bahn 
           file which contains this pipeline's modules, parameters, etc. Kraftwerk 
           is used to build bahn files into their pipeline file structure 
           where data will be added for processing.'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                description=textwrap.dedent(desc))
    parser.add_argument('-r', dest='reset', action='store_true', help="Reset loop")
    args = parser.parse_args()
    
    params = loadConfigFile( glob.glob('*.cfgYAML')[0] )
    if params:
        
        if not os.path.exists('autobahn.proc'): # exit if no autobahn.proc
            sys.exit('No autobahn.proc sqlite database. Rebuild autobahn.')

        # create connection to loop mechanism
        loop_con = sqlite3.connect('autobahn.proc') 
        loop_con.text_factory = str
        loop_con.row_factory = sqlite3.Row
        
        if args.reset: # do you want to reset loop and NOT auto-recover
            clear_loop(loop_con)

        con = sqlite3.connect( params['DB Name'] )
        main(loop_con, con, params)
    
    else: # exit if no params
        sys.exit('Unable to load config.yaml.')
