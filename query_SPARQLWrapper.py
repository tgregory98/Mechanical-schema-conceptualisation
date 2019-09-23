# import sys
# print(sys.executable)

from SPARQLWrapper import SPARQLWrapper, JSON

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setQuery("""
SELECT ?pred1 ?pred_inv1 ?n1 ?pred2 ?pred_inv2 ?n2 ?pred3 ?pred_inv3
WHERE {
	{ {
		FILTER(regex(?pred1, dct:subject)
		||regex(?pred1, skos:broader))
		dbc:Complex_systems_theory ?pred1 ?n1
	} UNION {
		FILTER(regex(?pred_inv1, dct:subject)
		||regex(?pred_inv1, skos:broader))
		?n1 ?pred_inv1 dbc:Complex_systems_theory
	} } .
	{ {
		FILTER(regex(?pred2, dct:subject)
		||regex(?pred2, skos:broader))
		?n1 ?pred2 ?n2
	} UNION	{
		FILTER(regex(?pred_inv2, dct:subject)
		||regex(?pred_inv2, skos:broader))
		?n2 ?pred_inv2 ?n1
	} } .
	{ {
		FILTER(regex(?pred3, dct:subject)
		||regex(?pred3, skos:broader))
		?n2 ?pred3 dbr:Word_embedding
	} UNION	{
		FILTER(regex(?pred_inv3, dct:subject)
		||regex(?pred_inv3, skos:broader))
		dbr:Word_embedding ?pred_inv3 ?n2
	} } .
}
""")

sparql.setReturnFormat("csv")
query_result = sparql.query()
print(query_result.geturl()) # Returns the url of the query
print(query_result.convert()) # Prints the result of the query

# 3-depth forward-back filtered search
# SELECT ?pred1 ?pred_inv1 ?n1 ?pred2 ?pred_inv2 ?n2 ?pred3 ?pred_inv3
# WHERE {
# 	{ {
# 		FILTER(regex(?pred1, dct:subject)
# 		||regex(?pred1, skos:broader))
# 		dbc:Complex_systems_theory ?pred1 ?n1
# 	} UNION {
# 		FILTER(regex(?pred_inv1, dct:subject)
# 		||regex(?pred_inv1, skos:broader))
# 		?n1 ?pred_inv1 dbc:Complex_systems_theory
# 	} } .
# 	{ {
# 		FILTER(regex(?pred2, dct:subject)
# 		||regex(?pred2, skos:broader))
# 		?n1 ?pred2 ?n2
# 	} UNION	{
# 		FILTER(regex(?pred_inv2, dct:subject)
# 		||regex(?pred_inv2, skos:broader))
# 		?n2 ?pred_inv2 ?n1
# 	} } .
# 	{ {
# 		FILTER(regex(?pred3, dct:subject)
# 		||regex(?pred3, skos:broader))
# 		?n2 ?pred3 dbr:Word_embedding
# 	} UNION	{
# 		FILTER(regex(?pred_inv3, dct:subject)
# 		||regex(?pred_inv3, skos:broader))
# 		dbr:Word_embedding ?pred_inv3 ?n2
# 	} } .
# }

# n3-depth forward-back search
# SELECT ?pred1 ?pred_inv1 ?n1 ?pred2 ?pred_inv2 ?n2 ?pred3 ?pred_inv3 ?n3
# WHERE {
# 	{ { dbc:Complex_systems_theory ?pred1 ?n1 } UNION { ?n1 ?pred_inv1 dbc:Complex_systems_theory } } .
# 	{ { ?n1 ?pred2 ?n2 } UNION { ?n2 ?pred_inv2 ?n1 } } .
# 	{ { ?n2 ?pred3 ?n3 } UNION { ?n3  ?pred_inv3 ?n2 } } .
# }

# n3-depth forward-back filtered search
# SELECT ?pred1 ?pred_inv1 ?n1 ?pred2 ?pred_inv2 ?n2 ?pred3 ?pred_inv3 ?n3
# WHERE {
# 	{ {
# 		FILTER(regex(?pred1, dct:subject)
# 		||regex(?pred1, skos:broader))
# 		dbc:Complex_systems_theory ?pred1 ?n1
# 	} UNION {
# 		FILTER(regex(?pred_inv1, dct:subject)
# 		||regex(?pred_inv1, skos:broader))
# 		?n1 ?pred_inv1 dbc:Complex_systems_theory
# 	} } .
# 	{ {
# 		FILTER(regex(?pred2, dct:subject)
# 		||regex(?pred2, skos:broader))
# 		?n1 ?pred2 ?n2
# 	} UNION	{
# 		FILTER(regex(?pred_inv2, dct:subject)
# 		||regex(?pred_inv2, skos:broader))
# 		?n2 ?pred_inv2 ?n1
# 	} } .
# 	{ {
# 		FILTER(regex(?pred3, dct:subject)
# 		||regex(?pred3, skos:broader))
# 		?n2 ?pred3 ?n3
# 	} UNION	{
# 		FILTER(regex(?pred_inv3, dct:subject)
# 		||regex(?pred_inv3, skos:broader))
# 		?n3 ?pred_inv3 ?n2
# 	} } .
# }

# n3-depth forward filtered search
# SELECT ?pred1 ?pred_inv1 ?n1 ?pred2 ?pred_inv2 ?n2 ?pred3 ?pred_inv3 ?n3
# WHERE {
# 	{ {
# 		FILTER(regex(?pred1, dct:subject)
# 		||regex(?pred1, skos:broader))
# 		dbc:Complex_systems_theory ?pred1 ?n1
# 	} UNION {
# 		FILTER(!IsIRI(?pred_inv1))
# 		?n1 ?pred_inv1 dbc:Complex_systems_theory
# 	} } .
# 	{ {
# 		FILTER(regex(?pred2, dct:subject)
# 		||regex(?pred2, skos:broader))
# 		?n1 ?pred2 ?n2
# 	} UNION {
# 		FILTER(!IsIRI(?pred_inv2))
# 		?n2 ?pred_inv2 ?n1
# 	} } .
# 	{ {
# 		FILTER(regex(?pred3, dct:subject)
# 		||regex(?pred3, skos:broader))
# 		?n2 ?pred3 ?n3
# 	} UNION {
# 		FILTER(!IsIRI(?pred_inv3))
# 		?n3 ?pred_inv3 ?n2
# 	} } .
# }

# n1-depth forward-back renaming search
# SELECT ?pred1_mod ?pred_inv1 ?n1_mod
# WHERE {
# 	{ { dbc:Complex_systems_theory ?pred1 ?n1 } UNION { ?n1 ?pred_inv1 dbc:Complex_systems_theory } } .
# 	BIND(strafter(str(?pred1), str(dct:)) AS ?pred1_mod)
# 	BIND(strafter(str(?n1), str(dbr:)) AS ?n1_mod)
# }
