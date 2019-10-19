# import sys
# print(sys.executable)


import modules.tr_funcs, modules.builders, modules.cleaners, modules.enrichers


print("\n-SCRIPT START-")

modules.tr_funcs.commit_cypher_query("""
MATCH (x)
DETACH DELETE (x)
    """)

url1 = "http://dbpedia.org/resource/Tea"
url2 = "http://dbpedia.org/resource/Milk"
url3 = "http://dbpedia.org/resource/Cup"


# -BUILDERS-
print("\n\n-BUILDING-")
print("\n-CATEGORIES")

count = {"nodes": 0, "edges": 0}
depth_constant_0 = 1
while count["nodes"] < 100 and count["edges"] < 100:
    print("depth_constant_0: " + str(depth_constant_0) + "\n")


    # PairwiseSchemaBuilder
    # sch1 = modules.builders.PairwiseSchemaBuilder(url1, url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # for i in range(depth_constant_0 - 1):
    #   sch1.run(i + 2)


    # ParentSchemaBuilder
    sch2a = modules.builders.ParentSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    sch2b = modules.builders.ParentSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    sch2c = modules.builders.ParentSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])

    sch2a.run(depth_constant_0)
    sch2b.run(depth_constant_0)
    sch2c.run(depth_constant_0)


    # PopulateSchemaBuilder
    # sch3a = modules.builders.PopulateSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # sch3b = modules.builders.PopulateSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # sch3c = modules.builders.PopulateSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])

    # sch3a.run(depth_constant_0)
    # sch3b.run(depth_constant_0)
    # sch3c.run(depth_constant_0)


    depth_constant_0 = depth_constant_0 + 1
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


print("\n-CLASSES")

depth_constant_1 = 1
nodes_added = 1
while nodes_added > 0:
    print("depth_constant_1: " + str(depth_constant_0) + "\n")

    nodes_before = modules.tr_funcs.commit_cypher_query_numpy("""
MATCH (x)
RETURN COUNT(x)
    """)

    sch4a = modules.builders.ParentSchemaBuilder(url1, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])
    sch4b = modules.builders.ParentSchemaBuilder(url2, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])
    sch4c = modules.builders.ParentSchemaBuilder(url3, filter_set_edges=["rdf:type", "rdfs:subClassOf", "rdfs:domain"], filter_set_vertices=["dbo:", "owl:"])

    sch4a.run(depth_constant_1)
    sch4b.run(depth_constant_1)
    sch4c.run(depth_constant_1)

    nodes_after = modules.tr_funcs.commit_cypher_query_numpy("""
MATCH (x)
RETURN COUNT(x)
    """)

    nodes_added = nodes_after[0][0] - nodes_before[0][0]
    print("nodes_added: " + str(nodes_added))
    depth_constant_1 = depth_constant_1 + 1


# -CLEANERS-
print("\n\n-CLEANING-")

# DisjointParentSchemaCleaner
cl1 = modules.cleaners.DisjointParentSchemaCleaner()
cl1.run(depth_constant_0)


# LeafSchemaCleaner
# cl2 = modules.cleaners.LeafSchemaCleaner()
# cl2.run(depth_constant_0)


# -ENRICHERS-
print("\n\n-ENRICHING-")

# IdsLabelsSchemaEnricher
en1 = modules.enrichers.IdsLabelsSchemaEnricher()
en1.run()


print("\n\n-SCRIPT COMPLETE-")
