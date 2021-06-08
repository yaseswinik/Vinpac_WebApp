# Vinpac_WebApp

#### app.py
Contains all the routes

#### fndef.py
Contains all the functions definitions for data preparation

#### visualfn.py
Contains functions for visualisation

#### mba.py
Contains all Association Analysis related functions 

## Changing confidence for rules
To change the confidence level for creating the rules, use the file mba.py. Make changes in min_threshold value in line 32:  grouped_rules_21 = association_rules(grouped_frequent_itemsets_21, metric="confidence", min_threshold=0.5)

To change confidence for unallocated and faulted states, change the min_threshold value in line 40: 
grouped_rules_21_4_5 = association_rules(grouped_frequent_itemsets_21, metric="confidence", min_threshold=0.10)

## Creating new container to run on docker
### Changes in app.py
In line 49: engine = sqlalchemy.create_engine('postgresql+psycopg2://admin:admin@localhost:5432/<new_schema_name>'), add the new database or schema name

In line 143: script = server_document(url='http://127.0.0.1:5006/dashboard') , change 5006 to new port number on which the dashboard is running and new dashboard file name used instead of dashboard for running the new dashboard. 

For running the dashboard use the link :  https://github.com/yaseswinik/Vinpac_Dashboard

In line 246: app.run(debug=True, host='0.0.0.0', port=<new_port_number>), change port number to new port number. 

### Creating the docker container

docker build -t <repo_>/<image_>:<tag_> .

To run the Docker container, 

docker run -p <new_port_number_for_container>:<port_number_specified_in_line_246> -d <repo_>/<image_>:<tag_>





 
