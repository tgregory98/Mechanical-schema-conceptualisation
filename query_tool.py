# import sys
# print(sys.executable)

from SPARQLWrapper import SPARQLWrapper
from py2neo import Database, Graph, Schema, Transaction, Cursor

def sparql_query_gen(start_page, end_page, depth):

	select_terms = ""
	for i in range(depth-1):
		i = i + 1
		string = "?pred& ?pred_inv& ?n& "
		select_terms = select_terms + string.replace("&", str(i))
	final_string = "?pred& ?pred_inv&"
	select_terms = select_terms + final_string.replace("&", str(depth))
	
	union_block = """
	{ {
		FILTER(regex(?pred1, dct:subject)
		||regex(?pred1, skos:broader))
		""" + start_page + """ ?pred1 ?n1
	} UNION {
		FILTER(regex(?pred_inv1, dct:subject)
		||regex(?pred_inv1, skos:broader))
		?n1 ?pred_inv1 """ + start_page + """
	} } .
	"""

	for i in range(depth-2):
		i = i + 1
		string = """
	{ {
		FILTER(regex(?pred&&, dct:subject)
		||regex(?pred&&, skos:broader))
		?n& ?pred&& ?n&&
	} UNION {
		FILTER(regex(?pred_inv&&, dct:subject)
		||regex(?pred_inv&&, skos:broader))
		?n&& ?pred_inv&& ?n&
	} } .
		"""
		union_block = union_block + string.replace("&&", str(i+1)).replace("&", str(i))

	final_union_block = """
	{ {
		FILTER(regex(?pred&&, dct:subject)
		||regex(?pred&&, skos:broader))
		?n& ?pred&& """ + end_page + """
	} UNION	{
		FILTER(regex(?pred_inv&&, dct:subject)
		||regex(?pred_inv&&, skos:broader))
		""" + end_page + """ ?pred_inv&& ?n&
	} } .
	"""
	union_block = union_block + final_union_block.replace("&&", str(depth)).replace("&", str(depth-1))

	query = "\rSELECT " + select_terms + "\rWHERE {\r" + union_block + "\r}"
	return query

query = sparql_query_gen("dbc:Complex_systems_theory", "dbr:Word_embedding",3)
wrapper = SPARQLWrapper("http://dbpedia.org/sparql")
wrapper.setQuery(query)

wrapper.setReturnFormat("csv")
query_result = wrapper.query()
url = query_result.geturl()

db = Database("bolt://localhost:7687", auth=("neo4j", "cayley"))
g = Graph("bolt://localhost:7687", auth=("neo4j", "cayley"))
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
