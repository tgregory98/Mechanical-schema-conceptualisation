# Mechanical-schema-conceptualisation

README IS CURRENTLY A WORK IN PROGRESS!

run.py is the build script which ties together the whole project. **Folder structure is as follows:**

## demo_schemas
Contains the results of some of the possible approaches. The results are png images exported from the Neo4j browser.
## modules
Contains the Python modules that were written to intelligently query the DBpedia SPARQL endpoint using SPARQL, convert this to .csv format, import this into the Neo4j database with Cypher, and build the graph representing the desired schema.
#### builders.py
The main script responsible for querying and building the intial graphs. This script currently also does a bit of enriching with the method cypher_query_set_gen. This enriching method should eventually be moved to enrichers.py.
#### cleaners.py
This script removes unwanted information from the graph.
#### enrichers.py
This script makes the data in the graph more readable and modifies the way the graph looks.
#### fetchers.py
Finally, this script fetches additional data for the graph once the underlying structure has been formed.
## old_scripts
Some of the original SPARQL and Cypher queries that I wrote. These scripts became what is now the builders.py script (which takes a more general approach). These scripts are retained just in case they come in handy in future.
