# import sys
# print(sys.executable)

from SPARQLWrapper import SPARQLWrapper
from py2neo import Database, Graph, Schema, Transaction, Cursor

sparql_query = """
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
"""

wrapper = SPARQLWrapper("http://dbpedia.org/sparql")
wrapper.setQuery(sparql_query)

wrapper.setReturnFormat("csv")
query_result = wrapper.query()
url = query_result.geturl()

db = Database("bolt://localhost:7687", auth=("neo4j","cayley"))
g = Graph("bolt://localhost:7687", auth=("neo4j","cayley"))
sch = Schema(g)

cypher_initialise = "WITH \"" + url + "\" AS url" + "\r" + """
LOAD CSV WITH HEADERS FROM url AS row
MERGE (a:page:start_page {iri: "http://dbpedia.org/resource/Category:Complex_systems_theory"})
MERGE (b:page {iri: row.n1})
MERGE (c:page {iri: row.n2})
MERGE (d:page:end_page {iri: "http://dbpedia.org/resource/Word_embedding"})

FOREACH (x IN CASE WHEN row.pred1 IS NULL THEN [] ELSE [1] END | MERGE (a)-[p:pred {iri: row.pred1}]->(b))
FOREACH (x IN CASE WHEN row.pred_inv1 IS NULL THEN [] ELSE [1] END | MERGE (a)<-[p:pred {iri: row.pred_inv1}]-(b))

FOREACH (x IN CASE WHEN row.pred2 IS NULL THEN [] ELSE [1] END | MERGE (b)-[p:pred {iri: row.pred2}]->(c))
FOREACH (x IN CASE WHEN row.pred_inv2 IS NULL THEN [] ELSE [1] END | MERGE (b)<-[p:pred {iri: row.pred_inv2}]-(c))

FOREACH (x IN CASE WHEN row.pred3 IS NULL THEN [] ELSE [1] END | MERGE (c)-[p:pred {iri: row.pred3}]->(d))
FOREACH (x IN CASE WHEN row.pred_inv3 IS NULL THEN [] ELSE [1] END | MERGE (c)<-[p:pred {iri: row.pred_inv3}]-(d))

RETURN a, b, c, d
"""

cypher_build_node_ids = """
MATCH (x)

SET (CASE left(x.iri,28) WHEN "http://dbpedia.org/resource/" THEN x END).id = toString(replace(x.iri,"http://dbpedia.org/resource/","")), (CASE left(x.iri,28) WHEN "http://dbpedia.org/resource/" THEN x END).prefix = "http://dbpedia.org/resource/"

SET (CASE left(x.iri,37) WHEN "http://dbpedia.org/resource/Category:" THEN x END).id = toString(replace(x.iri,"http://dbpedia.org/resource/Category:","")), (CASE left(x.iri,37) WHEN "http://dbpedia.org/resource/Category:" THEN x END).prefix = "http://dbpedia.org/resource/Category:"
"""

cypher_build_edge_ids = """
MATCH (x)-[y]->(z)

SET (CASE left(y.iri,25) WHEN "http://purl.org/dc/terms/" THEN y END).id = toString(replace(y.iri,"http://purl.org/dc/terms/","")), (CASE left(y.iri,25) WHEN "http://purl.org/dc/terms/" THEN y END).prefix = "http://purl.org/dc/terms/"

SET (CASE left(y.iri,36) WHEN "http://www.w3.org/2004/02/skos/core#" THEN y END).id = toString(replace(y.iri,"http://www.w3.org/2004/02/skos/core#","")), (CASE left(y.iri,36) WHEN "http://www.w3.org/2004/02/skos/core#" THEN y END).prefix = "http://www.w3.org/2004/02/skos/core#"
"""

cypher_build_article_labels = """
MATCH (x {prefix: "http://dbpedia.org/resource/"})
SET x:article
"""

cypher_build_category_labels = """
MATCH (x {prefix: "http://dbpedia.org/resource/Category:"})
SET x:category
"""

cypher_set = [cypher_initialise, cypher_build_node_ids, cypher_build_edge_ids, cypher_build_article_labels, cypher_build_category_labels]

tr = Transaction(g)
for cypher in cypher_set:
	tr.run(cypher)
tr.commit()
