# Schema-collect

**README IS CURRENTLY A WORK IN PROGRESS.** First note that run.py is the build script which ties together the whole project. Now here's a breakdown of the file and folder structure.

## demo_schemas folder
Contains the results of some of the possible approaches. The results are png images exported from the Neo4j browser.
## modules folder
Contains the Python modules that were written to (in this order):
1. Intelligently query the DBpedia SPARQL endpoint using SPARQL
2. Convert the output to .csv format
3. Import the .csv into the Neo4j database with Cypher
4. Build the graph representing the desired schema
5. Make the graph readable, relevant and visually intuitive
#### builders.py file
The main script responsible for querying and building the intial graphs. This script currently also does a bit of enriching with the method cypher_query_set_gen. This enriching method should eventually be moved to enrichers.py.
#### cleaners.py file
This script removes unwanted information from the graph.
#### enrichers.py file
This script makes the data in the graph more readable and modifies the way the graph looks.
#### fetchers.py file
Finally, this script fetches additional data for the graph once the underlying structure has been formed.
## old_scripts folder
Some of the original SPARQL and Cypher queries that I wrote. These scripts became what is now the builders.py script (which takes a more general approach). These scripts are retained just in case they come in handy in future.
