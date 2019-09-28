# import sys
# print(sys.executable)

import builders
from py2neo import Database, Graph, Transaction

db = Database("bolt://localhost:7687", auth=("neo4j", "cayley"))
g = Graph("bolt://localhost:7687", auth=("neo4j", "cayley"))


def commit_cypher_query(cypher_query):
    tr = Transaction(g)
    tr.run(cypher_query)
    tr.commit()


def commit_cypher_query_set(cypher_query_set):
    tr = Transaction(g)
    for cypher_query in cypher_query_set:
        tr.run(cypher_query)
    tr.commit()


commit_cypher_query("MATCH (x) DETACH DELETE (x)")

# sch1 = builders.PairwiseSchemaBuilder("http://dbpedia.org/resource/Television", "http://dbpedia.org/resource/Netflix", filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# commit_cypher_query_set(sch1.cypher_query_set_gen(2))
# commit_cypher_query_set(sch1.cypher_query_set_gen(3))
# commit_cypher_query_set(sch1.cypher_query_set_gen(4))

sch2a = builders.ParentSchemaBuilder("http://dbpedia.org/resource/Netflix", filter_set_edges=["dct:subject", "skos:broader", "rdf:type"], filter_set_vertices=[])
commit_cypher_query_set(sch2a.cypher_query_set_gen(2))

sch2b = builders.ParentSchemaBuilder("http://dbpedia.org/resource/Television", filter_set_edges=["dct:subject", "skos:broader", "rdf:type"], filter_set_vertices=[])
commit_cypher_query_set(sch2b.cypher_query_set_gen(2))

# TODO LIST
# + Clean up the code into one file.
# + Build a class for building pairwise schema.
# + Run a sequence of depth increasing queries.
# + Add a filter option to the class.
# + Test out different search filters and optimise.
# + Write a class for building parent schema.
# + Write an inheritance class for PairwiseSchemaBuilder and ParentSchemaBuilder.
# + Clean up code.
# + Add options for node/ edge specific filters.
# + Move some node/ edge filter code to SchemaBuilder class.
# + Split into two files, creating a builders.py module.
# + Create cleaners.py and fetchers.py files.
# - Create a PopulateSchemaBuilder subclass that populates all immediate neighbours.
# - Update the cypher_query_set_gen() strings.
# - Instead of using higher depths, try looking at immediate neighbours with many edges, and use them as the new start/end nodes from which to build new schema.
