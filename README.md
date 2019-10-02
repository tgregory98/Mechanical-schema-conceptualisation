# **Schema-structure**
##### Builds, cleans and enriches "schema" diagrams, connecting articles from across Wikipedia via the RDF DBpedia dataset.

###### A Python program that querys DBpedia's RDF dataset, a community maintained dataset based off Wikipedia, via the SPARQL endpoint. Then it uses this data to intelligently build "schema" representing the connections between articles from across Wikipedia. These schema are built with Cypher on the Neo4j graph database platform. The main aims of this program were to successfully wrangle the data and then to find clear schema representations, without overburdening the database. This program will be the first stage of a broader project.

## Showcase
###### ParentSchemaBuilder+DisjointParentSchemaCleaner
Image.

###### PairwiseSchemaBuilder
Image.

###### PopulateSchemaBuilder+LeafSchemaCleaner
Image.

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
