#!/bin/bash

# variable
dbname='yelp'
username='azuka'
working_dir=`pwd`

query="`cat yelp_*.sql`"

# Initiating Table Creation
psql $dbname $username << EOF 
$query
EOF

# Pushing CSV files
# Every csv file in flatfile will be into table based on its folder name
for file in `ls flatfile/`;
do
	csv_dir=`ls flatfile/$file/*.csv`
	query="COPY df_$file FROM '${working_dir}/${csv_dir}' DELIMITER ',' CSV HEADER;"
	psql $dbname $username << EOF 
	$query
EOF
done
