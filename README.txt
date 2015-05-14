The following two scripts are meant to export an aiida postgresql database to a Titan graph (which use hbase as a backend).

The postgresExportCSV.sh script allows you to export the main tables of an aiida postgresql database to CSV files.
The gremlinImportCSV.groovy script allows you to import the created CSV files to Titan db.

HOW TO:

To use the postgresExportCSV.sh script you first need to have a working installation of aiida and a postgresql aiida database
called aiidadb. To do so you can follow the aiida installation tutorial at http://aiida-core.readthedocs.org/en/stable/database/index.html

Once you ensured that the above requirements are fulfilled, run './postgresExportCSV.sh dir' where dir is the absolute path
to the directory where you want the CSV files to be stored. Then go to your titan installation directory and run './bin/gremlin.sh -e dir'
where dir is the absolute path to the directory where the generated CSV files lie.


