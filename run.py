# import sys
# print(sys.executable)


import time
import modules.misc, modules.etl, modules.schema


start_etl = time.time()
print("----- ETL START -----\n\n\n\n\n")

modules.misc.commit_cypher_query("""
MATCH (x)
DETACH DELETE (x)
    """)

url1 = "http://dbpedia.org/resource/Tea"
url2 = "http://dbpedia.org/resource/Milk"
url3 = "http://dbpedia.org/resource/Cup"


# -BUILDING-
print("\n\n\n\n\n----- BUILDING -----")
print("----- BUILDING / CATEGORIES -----\n\n\n\n\n")


# Pairwise
# pairwise_depth_constant = 4
# bui1a = modules.etl.Pairwise(url1, url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# for i in range(pairwise_depth_constant - 1):
#     print(i + 2)
#     bui1a.run(i + 2)
# bui1b = modules.etl.Pairwise(url2, url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# for i in range(pairwise_depth_constant - 1):
#     print(i + 2)
#     bui1b.run(i + 2)
# bui1c = modules.etl.Pairwise(url1, url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# for i in range(pairwise_depth_constant - 1):
#     print(i + 2)
#     bui1c.run(i + 2)


count = {"nodes": 0, "edges": 0}
depth_constant = 1
while count["nodes"] < 100 and count["edges"] < 100:
    print("depth_constant: " + str(depth_constant) + "\n")

    # Parent
    bui2a = modules.etl.Parent(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    bui2b = modules.etl.Parent(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    bui2c = modules.etl.Parent(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])

    bui2a.run(depth_constant)
    bui2b.run(depth_constant)
    bui2c.run(depth_constant)


    # Populate
    # bui3a = modules.etl.Populate(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # bui3b = modules.etl.Populate(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # bui3c = modules.etl.Populate(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])

    # bui3a.run(depth_constant)
    # bui3b.run(depth_constant)
    # bui3c.run(depth_constant)


    depth_constant = depth_constant + 1
    nodes = modules.misc.commit_cypher_query_numpy("""
MATCH (x)
RETURN COUNT(x)
    """)
    edges = modules.misc.commit_cypher_query_numpy("""
MATCH (x)-[r]->()
RETURN COUNT(r)
    """)
    count["nodes"] = nodes[0][0]
    count["edges"] = edges[0][0]
    print(count)


# print("\n\n\n\n\n----- BUILDING / CLASSES -----\n\n\n\n\n")

# bui4a = modules.etl.FiniteParent(url1, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])
# bui4b = modules.etl.FiniteParent(url2, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])
# bui4c = modules.etl.FiniteParent(url3, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])

# bui4a.run(6)
# bui4b.run(6)
# bui4c.run(6)


# -CLEANERS-
print("\n\n\n\n\n----- CLEANING -----\n\n\n\n\n")

# DisjointParent
cle1 = modules.etl.DisjointParent()
cle1.run(depth_constant)


# Leaf
# cle2 = modules.etl.Leaf()
# cle2.run(depth_constant)


print("\n\n\n\n\n----- ETL COMPLETE -----")
print(str(round(time.time() - start_etl, 1)) + " seconds.")

# -BENCHMARKING-
modules.misc.commit_cypher_query("""
MATCH (x)
SET x:etl
    """)
# modules.misc.commit_cypher_query("""
# CALL apoc.export.json.all("etl.json", {useTypes:true})
#     """)
# modules.misc.commit_cypher_query("""
# CALL apoc.load.json('etl.json') YIELD value
# RETURN value.type, value.properties, value.labels
#     """)




start_schema = time.time()
print("----- SCHEMA START -----\n\n\n\n\n")


fet1 = modules.schema.Meta(filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
fet1.run()


com1 = modules.schema.Structural([1, 1])
com1.run()


print("\n\n\n\n\n----- SCHEMA COMPLETE -----")
print(str(round(time.time() - start_schema, 1)) + " seconds.")




start_enrich = time.time()
print("----- ENRICH START -----\n\n\n\n\n")

enr1 = modules.misc.Enrich()
enr1.run()

print("\n\n\n\n\n----- ENRICH COMPLETE -----")
print(str(round(time.time() - start_enrich, 1)) + " seconds.")
