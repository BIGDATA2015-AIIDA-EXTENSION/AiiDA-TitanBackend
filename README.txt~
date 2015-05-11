The following files need to be present in your CSV directory in order to run the groovy script without errors:
	
	- attributes.csv
	- calcstates.csv
	- comments.csv
	- computers.csv
	- extras.csv
	- groups.csv
	- links.csv
	- nodegroups.csv
	- nodes.csv
	- users.csv

If one (or several) of those files does not have any entry ensure that the file is empty. In that case we recommend
to issue the following command 'touch comments.csv' (e.g if there is no comments yet).

To run the script first modify the first line of gremlinImportCSV.groovy so that csvDir is set to the directory where
your CSV files lie.

Finally run the script from your titan directory and issue the following command './bin/gremlin.sh -e path/to/gremlinImportCSV.groovy'
