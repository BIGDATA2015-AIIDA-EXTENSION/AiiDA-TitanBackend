#!/bin/bash

# Create necessary files for postgresql to export to and set owner 
# to user postgres

echo 'Creating files in directory '$1' ...'

sudo touch $1/attributes.csv
sudo chown postgres: $1/attributes.csv

sudo touch $1/calcstates.csv
sudo chown postgres: $1/calcstates.csv

sudo touch $1/comments.csv
sudo chown postgres: $1/comments.csv

sudo touch $1/computers.csv
sudo chown postgres: $1/computers.csv

sudo touch $1/extras.csv
sudo chown postgres: $1/extras.csv

sudo touch $1/groups.csv
sudo chown postgres: $1/groups.csv

sudo touch $1/links.csv
sudo chown postgres: $1/links.csv

sudo touch $1/nodegroups.csv
sudo chown postgres: $1/nodegroups.csv

sudo touch $1/nodes.csv
sudo chown postgres: $1/nodes.csv

sudo touch $1/users.csv
sudo chown postgres: $1/users.csv


# Issue queries to postgresql to export the necessary tables to the correct 
# csv files. The following commands assume that you have a working aiida 
# installation and that your aiida database is called aiidadb

echo 'Exporting db_dbattributes table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, key, datatype, regexp_replace(tval, '(\n|\r|\t|\b|\f|\v)', ' '), fval, ival, bval, dval, dbnode_id FROM db_dbattribute)
TO '$1/attributes.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbcalcstate table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, dbnode_id, state, time FROM db_dbcalcstate) TO '$1/calcstates.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbcomment table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, uuid, dbnode_id, ctime, mtime, user_id, regexp_replace(content, '(\n|\r|\t|\b|\f|\v)', ' ') FROM db_dbcomment) 
TO '$1/comments.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbcomputer table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, uuid, name, hostname, regexp_replace(description, '(\n|\r|\t|\b|\f|\v)', ' '), 
enabled, transport_type, scheduler_type, transport_params, metadata FROM db_dbcomputer) TO '$1/computers.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbextra table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, key, datatype, regexp_replace(tval, '(\n|\r|\t|\b|\f|\v)', ' '), fval, ival, bval, dval, dbnode_id FROM db_dbextra)
TO '$1/extras.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbgroup table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, uuid, name, type, time, regexp_replace(description, '(\n|\r|\t|\b|\f|\v)', ' '), user_id FROM db_dbgroup)
TO '$1/groups.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dblink table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, input_id, output_id, label FROM db_dblink) TO '$1/links.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbgroup_dbnodes table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, dbgroup_id, dbnode_id FROM db_dbgroup_dbnodes) TO '$1/nodegroups.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbnode table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, uuid, type, label, regexp_replace(description, '(\n|\r|\t|\b|\f|\v)', ' '), 
ctime, mtime, user_id, dbcomputer_id, nodeversion, public FROM db_dbnode) TO '$1/nodes.csv' DELIMITER ';' CSV;"

echo 'Exporting db_dbuser table ...'
sudo -u postgres psql -d aiidadb -c "COPY (SELECT id, password, last_login, is_superuser, email, first_name, last_name, institution, is_staff, is_active, date_joined FROM db_dbuser) 
TO '$1/users.csv' DELIMITER ';' CSV;"

# Set owner of the created files to the current user

sudo chown $USER: $1/attributes.csv
sudo chown $USER: $1/calcstates.csv
sudo chown $USER: $1/comments.csv
sudo chown $USER: $1/computers.csv
sudo chown $USER: $1/extras.csv
sudo chown $USER: $1/groups.csv
sudo chown $USER: $1/links.csv
sudo chown $USER: $1/nodegroups.csv
sudo chown $USER: $1/nodes.csv
sudo chown $USER: $1/users.csv

echo "Exporting properties ..."
sudo awk -F ";" '!seen[$2]++ {print $2 ";" $3}' $1/attributes.csv >> $1/properties.csv

echo "Exporting labels ..."
sudo awk -F ";" '!seen[$4]++ {print $4}' $1/links.csv >> $1/labels.csv


echo 'ALL TABLES HAVE BEEN EXPORTED SUCCESSFULLY'







