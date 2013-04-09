Kraftwerk
=========

Kraftwerk is a python utility which assembles bahn (previously called YASDBF) files which contain the modules used to stitch multiple mass spectrometry data formats into a SQLite results database. The bahn file is itself a compacted version of the code in a different SQLite database (not to be confused with a results database) that has a table structure representing the loop of the file processing pipeline being represented by the file. When the bahn file is extracted by Kraftwerk, it creates a folder heirachy with the codebase and an autobahn script. The autobahn creates an unrolled loop data structure in a table and uses this to process the proteomics data in folder. A small amount of configuration is allowed at this point via the config.yaml but most of the customization happens when building bahn files via Kraftwerk.
