# **Schema-structure**
##### Builds, cleans and enriches "schema" diagrams, connecting articles from across Wikipedia via the RDF DBpedia dataset.

###### A Python program that querys DBpedia's RDF dataset, a community maintained dataset based off Wikipedia, via the SPARQL endpoint. Then it uses this data to intelligently build "schema" representing the connections between articles from across Wikipedia. These schema are built with Cypher on the Neo4j graph database platform. The main aims of this program were to successfully wrangle the data, and then to rapidly find clear schema representations. This program will be the first stage of a broader project.

###### I took three alternative approaches, all of which are listed below in decreasing order of effectiveness. Most queries (in both SPARQL and Cypher) are dynamically generated based off the desired depth and the chosen root node. Where possible, I have also written built-in filter options. It all runs from one file using class instances, and it can go from a blank slate to a fully populated final schema in a matter of seconds.

## Showcase
#### 1. ParentSchemaBuilder+DisjointParentSchemaCleaner
![ParentSchemaBuilder+DisjointParentSchemaCleaner image]()

#### 2. PairwiseSchemaBuilder
![PairwiseSchemaBuilder image]()

#### 3. PopulateSchemaBuilder+LeafSchemaCleaner
![PopulateSchemaBuilder+LeafSchemaCleaner image]()

## File/ folder structure
- **demo_schemas** - contains the image results of some of the possible approaches.
- **modules** - contains the scripts which do most of the heavylifting.
    - builders.py - the main script responsible for querying and building the initial graphs.
    - cleaners.py - this script removes unwanted information from the graph.
    - enrichers.py - this script makes the data in the graph more readable and modifies the way the graph gets styled.
    - tr_funcs.py - this script provides database Transaction utilities for use across the project.
- TASK_LIST.md - a task list for personal use.
- run.py - the script that we 'run', and acts as a dashboard for arranging the various components of the graph we wish to build.
- style.grass - a loose file which may be uploaded to the Neo4j browser for personalised styling.
