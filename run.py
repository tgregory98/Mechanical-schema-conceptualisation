# import sys
# print(sys.executable)


import modules.tr_funcs, modules.builders, modules.cleaners, modules.enrichers


modules.tr_funcs.commit_cypher_query("""
MATCH (x)
DETACH DELETE (x)
    """)

url1 = "http://dbpedia.org/resource/Netflix"
url2 = "http://dbpedia.org/resource/Television"
url3 = "http://dbpedia.org/resource/Smart_TV"


# BUILDERS

# sch1 = modules.builders.PairwiseSchemaBuilder(url1, url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# sch1.run(2)
# sch1.run(3)
# sch1.run(4)


sch2a = modules.builders.ParentSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
sch2a.run(2)

sch2b = modules.builders.ParentSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
sch2b.run(2)

sch2c = modules.builders.ParentSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
sch2c.run(2)


# sch3a = modules.builders.PopulateSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# sch3a.run(1)

# sch3b = modules.builders.PopulateSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# sch3b.run(1)

# sch3c = modules.builders.PopulateSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# sch3c.run(1)


# CLEANERS

cl1 = modules.cleaners.DisjointParentSchemaCleaner()
cl1.run(2)

# cl2 = modules.cleaners.LeafSchemaCleaner()
# cl2.run(2)


# ENRICHERS

en1 = modules.enrichers.IdsLabelsSchemaEnricher()
en1.run()
