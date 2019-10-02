# **Schema-structure**
##### Builds, cleans and enriches "schema" diagrams, connecting articles from across Wikipedia via the RDF DBpedia dataset.

###### A Python program that querys DBpedia's RDF dataset, a community maintained dataset based off Wikipedia, via the SPARQL endpoint. Then it uses this data to intelligently build "schema" representing the connections between articles from across Wikipedia. These schema are built with Cypher on the Neo4j graph database platform. The main aims of this program were to successfully wrangle the data, and then to rapidly find clear schema representations. This program will be the first stage of a broader project.

###### I took three alternative approaches, all of which are listed below in decreasing order of effectiveness. Most queries (in both SPARQL and Cypher) are dynamically generated based off properties such as depth. Where possible, I have also written built-in filter options. It all runs from one file using class instances, and can go from a blank slate to a fully populated final schema in a matter of seconds.

## Showcase
#### 1. ParentSchemaBuilder+DisjointParentSchemaCleaner
![ParentSchemaBuilder+DisjointParentSchemaCleaner image](https://github.com/tgregory98/Schema-structure/blob/master/demo_schemas/ParentSchemaBuilder%2BDisjointParentSchemaCleaner%20(3%20root%20nodes).png)

#### 2. PairwiseSchemaBuilder
![PairwiseSchemaBuilder image](https://github.com/tgregory98/Schema-structure/blob/master/demo_schemas/PairwiseSchemaBuilder.png)

#### 3. PopulateSchemaBuilder+LeafSchemaCleaner
![PopulateSchemaBuilder+LeafSchemaCleaner image](https://github.com/tgregory98/Schema-structure/blob/master/demo_schemas/PopulateSchemaBuilder%2BLeafSchemaCleaner.png)

## File/ folder structure
- **demo_schemas** - contains the image results of some of the possible approaches.
- **modules** - contains the scripts which do most of the heavylifting.
    - builders.py - the main script responsible for querying and building the intial graphs.
    - cleaners.py - this script removes unwanted information from the graph.
    - enrichers.py - this script makes the data in the graph more readable and modifies the way the graph looks.
    - tr_funcs.py
- TASK_LIST.md
- run.py
- style.grass
