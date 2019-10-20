# import sys
# print(sys.executable)


import time
import modules.tr_funcs, modules.builders, modules.cleaners, modules.enrichers


start_time = time.time()
print("----- SCRIPT START -----")

modules.tr_funcs.commit_cypher_query("""
MATCH (x)
DETACH DELETE (x)
    """)

url1 = "http://dbpedia.org/resource/Tea"
url2 = "http://dbpedia.org/resource/Milk"
url3 = "http://dbpedia.org/resource/Cup"


# -BUILDERS-
print("\n\n\n\n\n----- BUILDING -----")
print("----- BUILDING / CATEGORIES -----")


# PairwiseSchemaBuilder
# pairwise_depth_constant = 4
# sch1a = modules.builders.PairwiseSchemaBuilder(url1, url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# for i in range(pairwise_depth_constant - 1):
#     print(i + 2)
#     sch1a.run(i + 2)
# sch1b = modules.builders.PairwiseSchemaBuilder(url2, url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# for i in range(pairwise_depth_constant - 1):
#     print(i + 2)
#     sch1b.run(i + 2)
# sch1c = modules.builders.PairwiseSchemaBuilder(url1, url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# for i in range(pairwise_depth_constant - 1):
#     print(i + 2)
#     sch1c.run(i + 2)


count = {"nodes": 0, "edges": 0}
depth_constant = 1
while count["nodes"] < 100 and count["edges"] < 100:
    print("depth_constant: " + str(depth_constant) + "\n")

    # ParentSchemaBuilder
    sch2a = modules.builders.ParentSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    sch2b = modules.builders.ParentSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    sch2c = modules.builders.ParentSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])

    sch2a.run(depth_constant)
    sch2b.run(depth_constant)
    sch2c.run(depth_constant)


    # PopulateSchemaBuilder
    # sch3a = modules.builders.PopulateSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # sch3b = modules.builders.PopulateSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # sch3c = modules.builders.PopulateSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])

    # sch3a.run(depth_constant)
    # sch3b.run(depth_constant)
    # sch3c.run(depth_constant)


    depth_constant = depth_constant + 1
    nodes = modules.tr_funcs.commit_cypher_query_numpy("""
MATCH (x)
RETURN COUNT(x)
    """)
    edges = modules.tr_funcs.commit_cypher_query_numpy("""
MATCH (x)-[r]->()
RETURN COUNT(r)
    """)
    count["nodes"] = nodes[0][0]
    count["edges"] = edges[0][0]
    print(count)


print("\n\n\n\n\n----- BUILDING / CLASSES -----")

sch4a = modules.builders.FiniteParentSchemaBuilder(url1, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])
sch4b = modules.builders.FiniteParentSchemaBuilder(url2, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])
sch4c = modules.builders.FiniteParentSchemaBuilder(url3, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])

sch4a.run(6)
sch4b.run(6)
sch4c.run(6)


# -CLEANERS-
print("\n\n\n\n\n----- CLEANING -----")

# DisjointParentSchemaCleaner
cl1 = modules.cleaners.DisjointParentSchemaCleaner()
cl1.run(depth_constant)


# LeafSchemaCleaner
# cl2 = modules.cleaners.LeafSchemaCleaner()
# cl2.run(depth_constant)


# -ENRICHERS-
print("\n\n\n\n\n----- ENRICHING -----")

# IdsLabelsSchemaEnricher
en1 = modules.enrichers.IdsLabelsSchemaEnricher()
en1.run()


print("\n\n\n\n\n----- SCRIPT COMPLETE -----")
print(str(round(time.time() - start_time, 1)) + " seconds.")
