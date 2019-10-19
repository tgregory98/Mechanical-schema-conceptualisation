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

count = {"nodes": 0, "edges": 0}
depth_constant = 1

while count["nodes"] < 100 and count["edges"] < 100:
    print("depth_constant = " + str(depth_constant) + "\n")


    # PairwiseSchemaBuilder
    # sch1 = modules.builders.PairwiseSchemaBuilder(url1, url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
    # for i in range(depth_constant - 1):
    #   sch1.run(i + 2)


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
    nodes = modules.tr_funcs.commit_cypher_query_data("""
MATCH (x)
RETURN COUNT(x)
    """)
    edges = modules.tr_funcs.commit_cypher_query_data("""
MATCH (x)-[r]->()
RETURN COUNT(r)
    """)
    count["nodes"] = nodes[0]["COUNT(x)"]
    count["edges"] = edges[0]["COUNT(r)"]
    print(count)


# -CLEANERS-
print("\n\n-CLEANING-")

# DisjointParentSchemaCleaner
cl1 = modules.cleaners.DisjointParentSchemaCleaner()
cl1.run(depth_constant)


# LeafSchemaCleaner
# cl2 = modules.cleaners.LeafSchemaCleaner()
# cl2.run(depth_constant)


# -ENRICHERS-
print("\n\n-ENRICHING-")

# IdsLabelsSchemaEnricher
en1 = modules.enrichers.IdsLabelsSchemaEnricher()
en1.run()


print("\n\n-SCRIPT COMPLETE-")
