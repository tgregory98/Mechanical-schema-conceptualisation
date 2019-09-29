# import sys
# print(sys.executable)


import modules.tr_funcs, modules.builders, modules.cleaners, modules.enrichers


modules.tr_funcs.commit_cypher_query("""
MATCH (x)
DETACH DELETE (x)
    """)

url1 = "http://dbpedia.org/resource/Netflix"
url2 = "http://dbpedia.org/resource/Television"
url3 = "http://dbpedia.org/resource/Category:Complex_systems_theory"
url4 = "http://dbpedia.org/resource/Category:Systems_theory"


# BUILDERS

# sch1 = modules.builders.PairwiseSchemaBuilder(url1, url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# sch1.build(2)
# sch1.build(3)
# sch1.build(4)


sch2a = modules.builders.ParentSchemaBuilder(url1, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
sch2a.build(3)

sch2b = modules.builders.ParentSchemaBuilder(url2, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
sch2b.build(3)


# sch3a = modules.builders.PopulateSchemaBuilder(url3, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# sch3a.build(1)

# sch3b = modules.builders.PopulateSchemaBuilder(url4, filter_set_edges=["dct:subject", "skos:broader"], filter_set_vertices=[])
# sch3b.build(1)


# CLEANERS

cl1 = modules.cleaners.LeafSchemaCleaner()
cl1.clean(3)

cl2 = modules.cleaners.DisjointParentSchemaCleaner()
cl2.clean()


# FETCHERS

# Work-in-progress


# ENRICHERS

en1 = modules.enrichers.IdsLabelsSchemaEnricher()
en1.enrich()
