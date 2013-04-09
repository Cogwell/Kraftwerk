#!/usr/bin/python
import sqlite3, argparse, textwrap, sys, os, zlib, shutil, glob
import yaml as Y

def initDB(args):
    # verify database doesn't already exist
    if os.path.isfile(args.bahn):
        sys.exit("Database already exists! Exiting.")
    
    # connect to database
    con = sqlite3.connect(args.bahn)
    
    # create tables
    con.execute("""CREATE TABLE modules
        (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, code BLOB)""")
    con.execute("""CREATE TABLE paths
        (id INTEGER PRIMARY KEY, dir VARCHAR NOT NULL, ext VARCHAR, generated BOOLEAN DEFAULT 0,
        CONSTRAINT uc_dirext UNIQUE(dir, ext) )""")
    con.execute("""CREATE TABLE db_parameters
        (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE NOT NULL, 
        value VARCHAR NOT NULL, description VARCHAR)""")
    con.execute("""CREATE TABLE processes
        (module_id INTEGER, position INTEGER UNIQUE NOT NULL, level INTEGER NOT NULL,
        function VARCHAR NOT NULL, inputs VARCHAR, path_id INTEGER NOT NULL,
        FOREIGN KEY(module_id) REFERENCES modules(id), FOREIGN KEY(path_id) REFERENCES paths(id) )""")
    con.execute("CREATE INDEX idx_module_id ON processes (module_id)")
    con.execute("CREATE INDEX idx_position ON processes (position)")

    # add baseDir as first entry in paths
    con.execute("""INSERT INTO paths (id, dir)
        VALUES (?,?)""", (0, args.baseDir) )

    # setup parameters
    # NOTE: some of these are globals and others aren't? Enzyme?
    params = [ ('Library Clustering', 'single', 'single/separate/structured'),
               ('DB Name', args.dbName, 'name of database file built by this pipeline'),
               #('YASDBF Version Number', '0.4', 'version number'),
               ('Enzyme', '1', "0-7; refer to msgf documentation on '-e' parameter"),
               ('Reuse Existing', 'True', 'Flag to determine if interimary files will be reused')]
    con.executemany("INSERT INTO db_parameters (name, value, description) VALUES(?,?,?)", params)

    con.commit()

def connectDB(db):
    # verify database does already exist
    if not os.path.isfile(db):
        sys.exit("Database does not exists! Exiting.")
    # connect to database
    return sqlite3.connect(db)

def compressFile(infile):
    with open(infile) as f:
        name = os.path.basename(infile)
        compressed_data = sqlite3.Binary(zlib.compress(f.read()))
    return (name, compressed_data)

def insertModule(args):
    con = connectDB(args.bahn)
    data = compressFile(args.path)
    try:
        con.execute("INSERT INTO modules (name, code) VALUES (?,?)", data)
        con.commit()
    except sqlite3.IntegrityError:
        sys.exit("Module already exists.") #Try updating.")

def insertPath(args):
    con = connectDB(args.bahn)
    try:
        con.execute("INSERT INTO paths (dir, ext, generated) VALUES (?,?,?)", (args.path, args.ext, args.generated) )
        con.commit()
    except sqlite3.IntegrityError:
        sys.exit("This path and extension are already related.")

def insertProcess(args):
    con = connectDB(args.bahn)
    try:
        inputs = args.inputs
        if inputs:
            inputs =  ', '.join(inputs)
        data = ( args.module_id, args.position, args.level, args.function, inputs, args.path_id )
        con.execute("""INSERT INTO processes 
            (module_id, position, level, function, inputs, path_id) 
            VALUES (?,?,?,?,?,?)""", data )
        con.commit()
    except sqlite3.IntegrityError:
        # TODO: make more descriptive
        sys.exit("Process already exists in bahn file")

def deleteModule(args):
    con = connectDB(args.bahn)
    for module in args.ids:
        con.execute("DELETE FROM modules WHERE id=?", (module,) )
    con.commit()

def updateModule(args):
    con = connectDB(args.bahn)
    cur = con.cursor()
    # compress data
    data = compressFile(args.path)
    cur.execute("SELECT * FROM modules WHERE name=?", (data[0],) )
    if cur.fetchone():
        cur.execute("UPDATE modules SET code=? WHERE name=?", data[::-1])
        con.commit()
    else:
        sys.exit("Module does not exist. Try inserting.")

def updateParams(args):
    con = connectDB(args.bahn)
    try:
        con.execute("UPDATE db_parameters SET value=? WHERE name=?", (args.value, args.name) )
        con.commit()
    except sqlite3.IntegrityError:
        raise
        #sys.exit("This path id is already in a rank loop. Try updating.")

def showModule(args):
    con = connectDB(args.bahn)
    cur = con.cursor()
    cur.execute("SELECT id, name, code FROM modules")
    
    # print heading and first rec
    rec=cur.fetchone()
    if rec:
        print "%s\t%s\t\t\t%s" % tuple(rec.keys())
        print "%i\t%s\tlen:" % (rec[0], rec[1]), len(rec[2])
        # print the rest of the recs
        for rec in cur:
            print "%i\t%s\tlen:" % (rec[0], rec[1]), len(rec[2])

def showParams(args):
    select = "SELECT name, value, description FROM db_parameters"
    print_results(args, select)

def showPath(args):
    select = "SELECT id, dir, ext FROM paths"
    print_results(args, select)

def print_results(args, select):
    # NOTE: make this prettier
    con = connectDB(args.bahn)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(select)
    # print heading and first rec
    rec = cur.fetchone()
    if rec:
        formatStr = ""
        for i in range(len(rec)):
            formatStr += "%s\t"
        formatStr = formatStr[:-1]
        print formatStr % tuple(rec.keys())
        print formatStr % tuple(rec)
        # print the rest of the recs
        for rec in cur:
            print formatStr % tuple(rec)

def deleteFile(f):
    try:
        os.remove(f)
    except OSError:
        pass

# Borrowed from Brian Clowers -- used to load yaml configuration file
def loadConfigFile(fileName):
    try:
        with open(fileName, 'r') as f:
            settings = Y.load(f)
        return settings
    except:
        print "error loading YAML Configuration File"
        return None

def buildAutobahn(args):
    con = connectDB(args.bahn)
    con.row_factory = sqlite3.Row # makes query results work simular to dict type
    cur = con.cursor()
    
    # query used to build file structure
    cur.execute("""SELECT dir FROM paths ORDER BY id ASC""")
  
    # pop base directory off the paths
    # NOTE: Consider putting this in a param...
    basePath = cur.fetchone()['dir'] # first entry is always basePath
    
    if True:
        os.chdir(basePath)
        
        # create directories for pipeline file structure
        for d in cur:
            path = d[0]
            if path != '.':
                if os.path.exists(path):
                    print "%s already exists." % path
                else:
                    os.makedirs(path)

        # determine clustering
        cur.execute("SELECT value FROM db_parameters WHERE name ='Library Clustering'")
        clustering = str(cur.fetchone()['value'])

        # create some place holders for structured format
        #if clustering == 'structured':

        # if config file then we want to extend it
        #cfg = os.path.join( os.getcwd(), glob.glob('*.cfgYAML')[0] )
        #params = loadConfigFile(cfg)
        cfg = 'autobahn.cfgYAML'
        params = []

        #NOTE: this needs to be finished in the if then cases...

        flag = False
        with open(cfg, 'a') as f:
            for rec in cur.execute("SELECT name, value, description FROM db_parameters"):
                if rec['name'] not in params:
                    if not flag:
                        f.write("# additional autobahn configuration starts here.\n")
                        flag = True
                    f.write("# %s\n" % (rec['description'],) )
                    f.write("%s: %s\n" % (rec['name'], rec['value']) )

        """
        # build config.yaml from db_parameters table
        with open('autobahn.cfgYAML', 'w') as f:
            f.write("# autobahn.cfgYAML used to configure autobahn.py\n")
            for rec in cur.execute("SELECT name, value, description FROM db_parameters"):
                f.write("# %s\n" % (rec['description'],) )
                f.write("%s: %s\n" % (rec['name'], rec['value']) )
        # change config.yaml permissions
        os.chmod('autobahn.cfgYAML', 0755)
        """

        # build info for dynamic looping 
        deleteFile('autobahn.proc') # NOTE: this may not be necessary
        con2 = sqlite3.connect('autobahn.proc')
        con2.execute("""CREATE TABLE processes
            (level INTEGER, name VARCHAR, function VARCHAR, inputs VARCHAR, 
            dir VARCHAR, ext VARCHAR, generated BOOLEAN)""")
        
        # gather looping data from bahn file
        data = []
        select = """SELECT a.level, c.name, a.function, a.inputs, b.dir, b.ext, b.generated FROM processes AS a
            JOIN paths AS b ON b.id=a.path_id
            JOIN modules AS c ON c.id=a.module_id
            ORDER BY a.position ASC"""
        for rec in con.execute(select):
            data.append(tuple(rec))
        con2.executemany("INSERT INTO processes VALUES (?,?,?,?,?,?,?)", data)
        con2.commit()

        try:
            os.makedirs('source')
        except OSError:
            pass
        
        # build source directory and unpack modules
        os.chdir('source')
        cur.execute("SELECT name, code FROM modules")
        for rec in cur:
            filename = rec[0]
            deleteFile(filename)
            with open(filename, 'w') as f:
                byte_data = bytes(rec[1])
                f.write(zlib.decompress(byte_data))
        
        # touch __init__.py so that directory is considered python module
        open("__init__.py",'w').close()
        
        # set autobahn permissions and move to baseDir
        deleteFile('../autobahn.py')
        os.chmod('autobahn.py', 0755)
        shutil.move('autobahn.py', '..')

if __name__ == "__main__":
    
    desc = '''\
           This utility is used to manage bahn files which are then expanded into autobahn processing pipelines.
           Bahn files are a sqlite database that have the outlining codebase to the 'way' one collects aggregates of 
           data and metrics into a single sqlite relational database. The bahn files then are expanded into autobahns
           which process the data put into the folder structure in an automated fashion. '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                description=textwrap.dedent(desc))
    parser.add_argument('bahn', type=str, help="Path to bahn file")
    subparser = parser.add_subparsers() #(help='sub-command help')
    
    # init (initalize) database
    init = subparser.add_parser('init', help="Used to initalize a new bahn file")
    init.add_argument('baseDir', type=str, help="Path to desired base directory for new database")
    init.add_argument('-db', dest='dbName', type=str, default='None', help="The name of the sqlite database being generated")
    init.set_defaults(func=initDB)
    
    # insert subgrouping
    insert = subparser.add_parser('insert', help="Used to insert info into a bahn file")
    sub_insert = insert.add_subparsers()

    # insert module
    insert_module = sub_insert.add_parser('module', help="Used to insert module into a bahn file")
    insert_module.add_argument('path', type=str, help="Path to module being inserted")
    insert_module.set_defaults(func=insertModule)
    
    # insert path
    insert_path = sub_insert.add_parser('path', help="Used to insert file location into a bahn file")
    insert_path.add_argument('path', type=str, help="Path to file location being stored")
    insert_path.add_argument('ext', type=str, help="File extension for files at given path")
    insert_path.add_argument('-g', dest='generated', action='store_true', help="Flag for denoting runtime generated files")
    insert_path.set_defaults(func=insertPath)
    
    # insert process
    insert_process = sub_insert.add_parser('process', help="Used to insert processes in to autobahn pipeline")
    insert_process.add_argument('module_id', type=int, help="Module id for process being inserted")
    insert_process.add_argument('position', type=int, help="Position of this code")
    insert_process.add_argument('level', type=int, help="Process level (0=database, 1=library, 2=file)")
    insert_process.add_argument('function', type=str, help="Name of function")
    insert_process.add_argument('-i', dest='inputs', type=str, nargs='+', help="Input variables (Seperated by spaces)")
    insert_process.add_argument('-path', dest='path_id', type=int, help="Id for path relating to this process", default=0)
    insert_process.set_defaults(func=insertProcess)
    
    # delete subgrouping
    delete = subparser.add_parser('delete', help="Used to delete info from a bahn file")
    sub_delete = delete.add_subparsers()
    
    # delete module
    delete_module = sub_delete.add_parser('module', help="Used to delete module from a bahn file")
    delete_module.add_argument('ids', metavar='N', type=int, nargs='+', help="Ids for modules being deleted") 
    delete_module.set_defaults(func=deleteModule)
    
    # update subgrouping
    update = subparser.add_parser('update', help="Used to update info in a bahn file")
    sub_update = update.add_subparsers()
    
    # update module
    update_module = sub_update.add_parser('module', help="Used to update module in a bahn file")
    update_module.add_argument('path', type=str, help="Path to module being updated")
    update_module.set_defaults(func=updateModule)

    # update db_parameters
    update_params = sub_update.add_parser('params', help="Used to update database parameters in a bahn file")
    update_params.add_argument('name', type=str, help="Name of database parameter to change")
    update_params.add_argument('value', type=str, help="New value for parameter")
    update_params.set_defaults(func=updateParams)
    
    # show subgrouping
    show = subparser.add_parser('show', help="Used to show data inside bahn file")
    sub_show = show.add_subparsers()
   
    # show module
    show_module = sub_show.add_parser('module', help="Used to show modules from a bahn file")
    show_module.set_defaults(func=showModule)

    # show params
    show_params = sub_show.add_parser('params', help="Used to show database parameters for a bahn file")
    show_params.set_defaults(func=showParams)

    # show path
    show_path = sub_show.add_parser('path', help="Used to show file location from a bahn file")
    show_path.set_defaults(func=showPath)

    # build autobahn
    build = subparser.add_parser('build', help="Used to build autobahn")
    build.set_defaults(func=buildAutobahn)

    args = parser.parse_args()
    args.func(args)

