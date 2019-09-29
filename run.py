# import sys
# print(sys.executable)

import modules.builders, modules.cleaners
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


commit_cypher_query("""
    MATCH (x)
    DETACH DELETE (x)
    """)

url1 = "http://dbpedia.org/resource/Netflix"
url2 = "http://dbpedia.org/resource/Television"
url3 = "http://dbpedia.org/resource/Category:Complex_systems_theory"
url4 = "http://dbpedia.org/resource/Category:Systems_theory"

# sch1 = modules.builders.PairwiseSchemaBuilder(url1, url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# commit_cypher_query_set(sch1.cypher_query_set_gen(2))
# commit_cypher_query_set(sch1.cypher_query_set_gen(3))
# commit_cypher_query_set(sch1.cypher_query_set_gen(4))


sch2a = modules.builders.ParentSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
commit_cypher_query_set(sch2a.cypher_query_set_gen(3))

sch2b = modules.builders.ParentSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
commit_cypher_query_set(sch2b.cypher_query_set_gen(3))


# sch3a = modules.builders.PopulateSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# commit_cypher_query_set(sch3a.cypher_query_set_gen(1))

# sch3b = modules.builders.PopulateSchemaBuilder(url4, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# commit_cypher_query_set(sch3b.cypher_query_set_gen(1))


cl1 = modules.cleaners.LeafSchemaCleaner([url1, url2])
commit_cypher_query_set(cl1.cypher_query_set_gen(3))
