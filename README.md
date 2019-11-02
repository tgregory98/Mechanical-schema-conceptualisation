# **Schema-dreamer**
# Release 0.2.1. Schema setup
**Builds, cleans and enriches "schema" diagrams, connecting articles from across Wikipedia via the RDF DBpedia dataset.**

A Python program which queries DBpedia's massive RDF dataset, a community maintained dataset based off Wikipedia, via the SPARQL endpoint. Then it uses this data to intelligently build "schema" representing the connections between articles from across Wikipedia. These schema are built with Cypher on the Neo4j graph database platform. It all runs from one file using class instances, and it can go from a blank slate to a fully populated final schema in a matter of seconds.

The main aims of this program were to successfully extract the data, transform it, load it into the Neo4j platform and then to rapidly find clear schema representations. This program will be the first stage of a broader project inspired by the roots of [constructivism](https://en.wikipedia.org/wiki/Constructivism_(philosophy_of_education)) (or more specifically [schemas](https://en.wikipedia.org/wiki/Schema_(psychology)), which are *not* to be confused with database schemas). Here is a preview of what can be currently achieved.

![image](https://github.com/tgregory98/Schema-dreamer/blob/master/demo_schemas/preview)

At this stage I am working on adding NLP, entity resolution and concurrency. Release 0.2.1 is focusing on setting up for these features. What follows is a breakdown of each stage of the project, in chronological order.

1. **ETL:** This stage involves querying and building the initial graph, and cleaning unwanted nodes. I took four alternative compound approaches for each ETL release, all of which are listed and fully explained in in 0.1.0 and 0.2.0 showcases, in decreasing order of effectiveness. Most queries (in both SPARQL and Cypher) are dynamically generated based off the desired depth and the chosen root node. I have also written built-in filter options.
	- Querying from SPARQL endpoint
	- Building graph
	- Cleaning unwanted nodes

2. **SCHEMA:** The next stage of the process, which populates the graph with metadata, computes similarity scores, performs entity recognition, and finally abstracts the structure.

	- Populating `etl` nodes from previous stage with `meta` nodes
	- (Work-in-progress) Computing an pairwise similarity score matrix based of the `meta` nodes, aggregated from:
		- a structural similarity matrix (taking a very contextual approach)
		- a inherent similarity matrix (a standard approach)
	- (Work-in-progress) Deduplication of `etl` nodes
	- (Work-in-progress) Abstracting the graph by removing very contextual nodes

   The interesting maths comes in with the computation of the structural similarity matrix, so I will explain it here. Once this is complete and working with arbitrary hyperparameters, I may train this with a machine learning model.
   
   > ![](https://latex.codecogs.com/gif.latex?%5Cinline%20%5C%5C%20%5Ctext%7BLet%20%24G%3D%28V%2CE%29%24%20be%20a%20graph%20where%20%24V%3DV%28G%29%2CE%3DE%28G%29%24%2C%7D%5C%5C%20%5Ctext%7B%24N%2CM%24%20be%20the%20sets%20of%20%27etl%27%20and%20%27meta%27%20nodes%20respectively%2C%20where%20%24N%5Ccup%20M%3DV%28G%29%24%7D%5C%5C%20%5Ctext%7B%28%24N%24%20stands%20for%20%5Cunderline%7Bnewly%7D%20generated%20nodes%2C%20and%20%24M%24%20stands%20for%20%5Cunderline%7Bmeta%7D%20nodes%29.%7D%5C%5C%20%5Ctext%7BLet%20%24n%5Cin%20N%2Cm%5Cin%20M%24%20be%20singular%20%27etl%27%20and%20%27meta%27%20nodes%20respectively%2C%7D%5C%5C%20%24p_m%3D%28%5Ctext%7B%5C%23%20etl%20nodes%20parent%20to%20%24m%24%7D%29%2Cc_m%3D%28%5Ctext%7B%5C%23%20etl%20nodes%20child%20to%20%24m%24%7D%29%2Cn%3D%5Ctext%7B%28%5C%23%20etl%20nodes%20in%20total%29%7D.%5C%5C%20%5Ctext%7BFinally%20let%20%24%5Clambda_p%2C%5Clambda_c%5Cin%5Cmathbb%7BR%7D%24%20be%20hyperparameters.%7D)
   
   > ![](https://latex.codecogs.com/gif.latex?%5Cinline%20%5C%5C%20%5Ctext%7BFor%20each%20%24m%5Cin%20M%24%3A%7D%5C%5C%20%5Chspace*%7B1cm%7D%5Ctext%7Bfind%20%5C%25%20coverage%20of%20%24m%24%20given%20by%20%7D%5Calpha_m%3A%3D%5Cfrac%7B%5Clambda_pp_m&plus;%5Clambda_cc_m%7D%7Bn%7D%5C%5C%20%5Chspace*%7B1cm%7D%5Ctext%7Bthen%20take%20the%20reciprocal%2C%20representing%20the%20uniqueness%20%7D%5Cfrac%7B1%7D%7B%5Calpha_m%7D)
   
   > ![](https://latex.codecogs.com/gif.latex?%5Cinline%20%5C%5C%20%5Ctext%7BFor%20each%20%24n_1%2Cn_2%5Cin%20N%24%3A%7D%5C%5C%20%5Chspace*%7B1cm%7D%5Ctext%7Blet%20%7DM%27_%7Bn_1%2Cn_2%7D%3D%5C%7Bm%5Cin%20M%3An_1m%5Cin%20E%28G%29%5Ctext%7B%20and%20%7Dn_2m%5Cin%20E%28G%29%5C%7D%5C%5C%20%5Chspace*%7B1cm%7D%5Ctext%7Bthen%20%7DS%28n_1%2Cn_2%29%3D%5Csum_%7Bm%5Cin%20M%27_%7Bn_1%2Cn_2%7D%7D%5Cfrac%7B1%7D%7B%5Calpha_m%7D)
   
   > ![](https://latex.codecogs.com/gif.latex?%5Cinline%20%5C%5C%20%5Ctext%7BThis%20yields%20the%20structural%20similarity%20matrix%20%24S%3D%28S%28n_1%2Cn_2%29%29_%7Bn_1%2Cn_2%5Cin%20N%7D%24%20or%20in%20full%3A%7D%5C%5C%20%5Chspace*%7B1cm%7DS%3D%5Cleft%28%5Csum_%7Bm%5Cin%5C%7Bm%5Cin%20M%3An_1m%5Cin%20E%28G%29%5Ctext%7B%20and%20%7Dn_2m%5Cin%20E%28G%29%5C%7D%7D%5Cfrac%7Bn%7D%7B%5Clambda_pp_m&plus;%5Clambda_cc_m%7D%5Cright%29_%7Bn_1%2Cn_2%5Cin%20N%7D%5C%5C%20%5Chspace*%7B1cm%7D%5Ctext%7Bwhere%20%7DG%3D%28N%5Ccup%20M%2CE%28G%29%29%2C%5Cquad%5Clambda_p%2C%5Clambda_c%5Cin%5Cmathbb%7BR%7D)
   
   For the inherent similarity matrix, I will probably use an affine gap distance metric. To aggregate these two matrices into one, I may use a meta-learning algorithm to decide on an optimal linear combination.
   
   Finally, to perform abstraction, I will figure out a valid metric to represent how context invariant each node is, and then I will use a one-tailed significance test to determine which nodes must be omitted.

3. (Work-in-progress).

## File/ folder structure
- **demo_schemas**: Contains the image results of some of the possible approaches.
- **modules**: Contains the scripts which do most of the heavylifting.
    - etl.py: The script responsible for querying and building the initial graph, and cleaning unwanted nodes.
    - schema.py: The next stage of the process, which populates the graph with metadata, computes similarity scores, performs entity recognition, and finally abstracts the structure.
    - misc.py: This script provides database Transaction utilities and enrichment objects for use across the entire project.
- TASK_LIST.md: A task list for personal use.
- run.py: The script that we 'run', and acts as a dashboard for arranging the various components of the graph we wish to build.
- style.grass: A loose file which may be uploaded to the Neo4j browser for personalised styling.

## 0.2.0. Improved ETL Showcase
Screenshots of some of the outputs generated by the three alternative approaches, ran on three different Wikipedia articles: Tea, Milk and Cup.
#### 1. ParentSchemaBuilder + FiniteParentSchemaBuilder + DisjointParentSchemaCleaner (dynamic depth, root_nodes=3) (26.6 seconds)
![image](https://github.com/tgregory98/Schema-dreamer/blob/master/demo_schemas/ParentSchemaBuilder%20%2B%20FiniteParentSchemaBuilder%20%2B%20DisjointParentSchemaCleaner%20(dynamic%20depth%2C%20root_nodes%3D3)%20(26.6%20seconds).png)

This algorithm performs the best in terms of clarity of representation and scalability. However it is now the slowest of the four approaches. It finds parent categories until the root node trees intersect and we get a connection (or rather, these branches get temporarily tagged and so they don't get removed by the cleaner class).

#### 2. PopulateSchemaBuilder + FiniteParentSchemaBuilder + LeafSchemaCleaner (dynamic depth, root_nodes=3) (25.2 seconds)
![image](https://github.com/tgregory98/Schema-dreamer/blob/master/demo_schemas/PopulateSchemaBuilder%20%2B%20FiniteParentSchemaBuilder%20%2B%20LeafSchemaCleaner%20(dynamic%20depth%2C%20root_nodes%3D3)%20(25.2%20seconds).png)

This is essentially what happens if you run the first approach, except you consider not just parents but also children. This was most similar to the least effective approach in the previous release, however the dynamic depth feature has pushed up the effectiveness to 2nd place. DisjointParentSchemaCleaner has also been replaced with LeafSchemaCleaner. It hasn't been thoroughly tested yet but it is suspected that it may not perform quite as well on different root sets.

#### 3. ParentSchemaBuilder + FiniteParentSchemaBuilder + LeafSchemaCleaner (dynamic depth, root_nodes=3) (23.6 seconds)
![image](https://github.com/tgregory98/Schema-dreamer/blob/master/demo_schemas/ParentSchemaBuilder%20%2B%20FiniteParentSchemaBuilder%20%2B%20LeafSchemaCleaner%20(dynamic%20depth%2C%20root_nodes%3D3)%20(23.6%20seconds).png)

If you replace the DisjointParentSchemaCleaner from the first approach with LeafSchemaCleaner, then you can increase its speed. This one finds parent categories and then iteratively removes leafs for a fixed number of passes. It is only the trees attached to a singular root that are removed by LeafSchemaCleaner. The result is similar to the first approach, except it preserves clusters of nodes around singular roots. As a result, the data is a bit noisy.

#### 4. PairwiseSchemaBuilder + FiniteParentSchemaBuilder (depth=2,3,4, root_nodes=3) (10.7 seconds)
![image](https://github.com/tgregory98/Schema-dreamer/blob/master/demo_schemas/PairwiseSchemaBuilder%20%2B%20FiniteParentSchemaBuilder%20(depth%3D2%2C3%2C4%2C%20root_nodes%3D3)%20(10.7%20seconds).png)

This one isn't quite as useful as the previous in my opinion because the representations aren't as meaningful nor as scalable. Sometimes the complexity explodes because it looks at both children and parents. It doesn't perform too well on this root set.

## 0.1.0. ETL Showcase
Screenshots of some of the outputs generated by the three alternative approaches, ran on three different Wikipedia articles: Netflix, Television and Smart_TV.
#### 1. ParentSchemaBuilder + DisjointParentSchemaCleaner (depth=2, root_nodes=3) (8.3 seconds)
![image](https://github.com/tgregory98/Schema-structure/blob/master/demo_schemas/ParentSchemaBuilder%20%2B%20DisjointParentSchemaCleaner%20(depth%3D2%2C%20root_nodes%3D3)%20(8.3%20seconds).png)

This algorithm performs the best in terms of clarity of representation, speed and scalability. It finds parent categories until the root node trees intersect and we get a connection (or rather, these branches get temporarily tagged and so they don't get removed by the cleaner class). Going forward, this will be the algorithm of choice.

#### 2. ParentSchemaBuilder + LeafSchemaCleaner (depth=2, root_nodes=3) (5.4 seconds)
![image](https://github.com/tgregory98/Schema-structure/blob/master/demo_schemas/ParentSchemaBuilder%20%2B%20LeafSchemaCleaner%20(depth%3D2%2C%20root_nodes%3D3)%20(5.4%20seconds).png)

If you replace the DisjointParentSchemaCleaner from above with LeafSchemaCleaner then you can increase the speed of the first algorithm. This one finds parent categories and then iteratively removes leafs for a fixed number of passes. It is only the trees attached to a singular root that are removed by LeafSchemaCleaner. The result is similar to the above, except it preserves clusters of nodes around singular roots.

#### 3. PairwiseSchemaBuilder (depth=2,3,4, root_nodes=2) (5.0 seconds)
![image](https://github.com/tgregory98/Schema-structure/blob/master/demo_schemas/PairwiseSchemaBuilder%20(depth%3D2%2C3%2C4%2C%20root_nodes%3D2)%20(5.0%20seconds).png)

This one isn't quite as useful as the previous two in my opinion because the representations aren't as meaningful nor as scalable. Sometimes the complexity explodes because it looks at both children and parents.

#### 4. PopulateSchemaBuilder + DisjointParentSchemaCleaner (depth=1, root_nodes=3) (8.5 seconds)
![image](https://github.com/tgregory98/Schema-structure/blob/master/demo_schemas/PopulateSchemaBuilder%20%2B%20DisjointParentSchemaCleaner%20(depth%3D1%2C%20root_nodes%3D3)%20(8.5%20seconds).png)

Finally, we have the least effective approach. This is essentially what happens if you run the first approach, except you consider not just parents but also children. The complexity explodes very fast so I had to limit the depth of this search to 1. The only reason I have kept this around is because it is sometimes useful for data exploration.

