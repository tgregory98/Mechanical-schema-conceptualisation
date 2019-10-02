# **Schema-structure**
##### Builds, cleans and enriches "schema" diagrams, connecting articles from across Wikipedia via the RDF DBpedia dataset.

###### A Python program that querys DBpedia's RDF dataset, a community maintained dataset based off Wikipedia, via the SPARQL endpoint. Then it uses this data to intelligently build "schema" representing the connections between articles from across Wikipedia. These schema are built with Cypher on the Neo4j graph database platform. The main aim of this program was finding clear schema representations that are not compute intensive. This program will be the first stage of a broader project.

## Showcase
###### ParentSchemaBuilder+DisjointParentSchemaCleaner
Image.

###### PairwiseSchemaBuilder
Image.

###### PopulateSchemaBuilder+LeafSchemaCleaner
Image.

## File/ folder structure
- **demo_schemas**
- **modules**
    - builders.py
    - cleaners.py
    - enrichers.py
    - tr_funcs.py
- **old_scripts**
- TASK_LIST.md
- run.py
- style.grass
