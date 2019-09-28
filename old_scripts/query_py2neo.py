# import sys
# print(sys.executable)

from py2neo import Database, Graph, Transaction, Cursor
db = Database("bolt://localhost:7687", auth=("neo4j", "cayley"))
g = Graph("bolt://localhost:7687", auth=("neo4j", "cayley"))

cypher_match_all = """
MATCH (x)
RETURN (x)
"""

cypher_delete_all = """
MATCH (x)
DETACH DELETE (x)
"""

cypher_n3depth_forward_filtered_search = """
WITH "http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=SELECT+%3Fpred1+%3Fpred_inv1+%3Fn1+%3Fpred2+%3Fpred_inv2+%3Fn2+%3Fpred3+%3Fpred_inv3+%3Fn3%0D%0AWHERE+%7B%0D%0A%09%7B+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred1%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred1%2C+skos%3Abroader%29%29%0D%0A%09%09dbc%3AComplex_systems_theory+%3Fpred1+%3Fn1%0D%0A%09%7D+UNION+%7B%0D%0A%09%09FILTER%28%21IsIRI%28%3Fpred_inv1%29%29%0D%0A%09%09%3Fn1+%3Fpred_inv1+dbc%3AComplex_systems_theory%0D%0A%09%7D+%7D+.%0D%0A%09%7B+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred2%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred2%2C+skos%3Abroader%29%29%0D%0A%09%09%3Fn1+%3Fpred2+%3Fn2%0D%0A%09%7D+UNION+%7B%0D%0A%09%09FILTER%28%21IsIRI%28%3Fpred_inv2%29%29%0D%0A%09%09%3Fn2+%3Fpred_inv2+%3Fn1%0D%0A%09%7D+%7D+.%0D%0A%09%7B+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred3%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred3%2C+skos%3Abroader%29%29%0D%0A%09%09%3Fn2+%3Fpred3+%3Fn3%0D%0A%09%7D+UNION+%7B%0D%0A%09%09FILTER%28%21IsIRI%28%3Fpred_inv3%29%29%0D%0A%09%09%3Fn3+%3Fpred_inv3+%3Fn2%0D%0A%09%7D+%7D+.%0D%0A%7D&format=text%2Fcsv&CXML_redir_for_subjs=121&CXML_redir_for_hrefs=&timeout=30000&debug=on&run=+Run+Query+" AS url


LOAD CSV WITH HEADERS FROM url AS row
MERGE (a:page:start_page {iri: "http://dbpedia.org/resource/Category:Complex_systems_theory"})
MERGE (b:page {iri: row.n1})
MERGE (c:page {iri: row.n2})
MERGE (d:page {iri: row.n3})

FOREACH (x IN CASE WHEN row.pred1 IS NULL THEN [] ELSE [1] END | MERGE (a)-[p:pred {iri: row.pred1}]->(b))

FOREACH (x IN CASE WHEN row.pred2 IS NULL THEN [] ELSE [1] END | MERGE (b)-[p:pred {iri: row.pred2}]->(c))

FOREACH (x IN CASE WHEN row.pred3 IS NULL THEN [] ELSE [1] END | MERGE (c)-[p:pred {iri: row.pred3}]->(d))

RETURN a, b, c, d
"""

cypher_n1depth_forwardback_filtered_search = """
WITH "http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=SELECT+%3Fpred1+%3Fpred_inv1+%3Fn1%0D%0AWHERE+%7B%0D%0A%09%7B+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred1%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred1%2C+skos%3Abroader%29%29%0D%0A%09%09dbc%3AComplex_systems_theory+%3Fpred1+%3Fn1%0D%0A%09%7D+UNION+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred_inv1%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred_inv1%2C+skos%3Abroader%29%29%0D%0A%09%09%3Fn1+%3Fpred_inv1+dbc%3AComplex_systems_theory%0D%0A%09%7D+%7D+.%0D%0A%7D&format=text%2Fcsv&CXML_redir_for_subjs=121&CXML_redir_for_hrefs=&timeout=30000&debug=on&run=+Run+Query+" AS url


LOAD CSV WITH HEADERS FROM url AS row
MERGE (a:page:start_page { iri: "http://dbpedia.org/resource/Category:Complex_systems_theory"})
MERGE (b:page {iri: row.n1})

FOREACH (x IN CASE WHEN row.pred1 IS NULL THEN [] ELSE [1] END | MERGE (a)-[p:pred {iri: row.pred1}]->(b))
FOREACH (x IN CASE WHEN row.pred_inv1 IS NULL THEN [] ELSE [1] END | MERGE (a)-[p:pred_inv {iri: row.pred_inv1}]->(b))

RETURN a, b
"""

cypher_3depth_forwardback_filtered_search = """
WITH "http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=SELECT+%3Fpred1+%3Fpred_inv1+%3Fn1+%3Fpred2+%3Fpred_inv2+%3Fn2+%3Fpred3+%3Fpred_inv3%0D%0AWHERE+%7B%0D%0A%09%7B+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred1%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred1%2C+skos%3Abroader%29%29%0D%0A%09%09dbc%3AComplex_systems_theory+%3Fpred1+%3Fn1%0D%0A%09%7D+UNION+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred_inv1%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred_inv1%2C+skos%3Abroader%29%29%0D%0A%09%09%3Fn1+%3Fpred_inv1+dbc%3AComplex_systems_theory%0D%0A%09%7D+%7D+.%0D%0A%09%7B+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred2%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred2%2C+skos%3Abroader%29%29%0D%0A%09%09%3Fn1+%3Fpred2+%3Fn2%0D%0A%09%7D+UNION%09%7B%0D%0A%09%09FILTER%28regex%28%3Fpred_inv2%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred_inv2%2C+skos%3Abroader%29%29%0D%0A%09%09%3Fn2+%3Fpred_inv2+%3Fn1%0D%0A%09%7D+%7D+.%0D%0A%09%7B+%7B%0D%0A%09%09FILTER%28regex%28%3Fpred3%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred3%2C+skos%3Abroader%29%29%0D%0A%09%09%3Fn2+%3Fpred3+dbr%3AWord_embedding%0D%0A%09%7D+UNION%09%7B%0D%0A%09%09FILTER%28regex%28%3Fpred_inv3%2C+dct%3Asubject%29%0D%0A%09%09%7C%7Cregex%28%3Fpred_inv3%2C+skos%3Abroader%29%29%0D%0A%09%09dbr%3AWord_embedding+%3Fpred_inv3+%3Fn2%0D%0A%09%7D+%7D+.%0D%0A%7D&format=text%2Fcsv&CXML_redir_for_subjs=121&CXML_redir_for_hrefs=&timeout=30000&debug=on&run=+Run+Query+" AS url


LOAD CSV WITH HEADERS FROM url AS row
MERGE (a:page:start_page {iri: "http://dbpedia.org/resource/Category:Complex_systems_theory"})
MERGE (b:page {iri: row.n1})
MERGE (c:page {iri: row.n2})
MERGE (d:page:end_page {iri: "http://dbpedia.org/resource/Word_embedding"})

FOREACH (x IN CASE WHEN row.pred1 IS NULL THEN [] ELSE [1] END | MERGE (a)-[p:pred {iri: row.pred1}]->(b))
FOREACH (x IN CASE WHEN row.pred_inv1 IS NULL THEN [] ELSE [1] END | MERGE (a)-[p:pred_inv {iri: row.pred_inv1}]->(b))

FOREACH (x IN CASE WHEN row.pred2 IS NULL THEN [] ELSE [1] END | MERGE (b)-[p:pred {iri: row.pred2}]->(c))
FOREACH (x IN CASE WHEN row.pred_inv2 IS NULL THEN [] ELSE [1] END | MERGE (b)-[p:pred_inv {iri: row.pred_inv2}]->(c))

FOREACH (x IN CASE WHEN row.pred3 IS NULL THEN [] ELSE [1] END | MERGE (c)-[p:pred {iri: row.pred3}]->(d))
FOREACH (x IN CASE WHEN row.pred_inv3 IS NULL THEN [] ELSE [1] END | MERGE (c)-[p:pred_inv {iri: row.pred_inv3}]->(d))

RETURN a, b, c, d
"""

cypher_build_node_ids = """
MATCH (x)

SET (CASE left(x.iri,28) WHEN "http://dbpedia.org/resource/" THEN x END).id = toString(replace(x.iri,"http://dbpedia.org/resource/","")), (CASE left(x.iri,28) WHEN "http://dbpedia.org/resource/" THEN x END).prefix = "http://dbpedia.org/resource/"

SET (CASE left(x.iri,37) WHEN "http://dbpedia.org/resource/Category:" THEN x END).id = toString(replace(x.iri,"http://dbpedia.org/resource/Category:","")), (CASE left(x.iri,37) WHEN "http://dbpedia.org/resource/Category:" THEN x END).prefix = "http://dbpedia.org/resource/Category:"
"""

cypher_build_article_labels = """
MATCH (x {prefix: "http://dbpedia.org/resource/"})
SET x:article
"""

cypher_build_category_labels = """
MATCH (x {prefix: "http://dbpedia.org/resource/Category:"})
SET x:category
"""

tr = Transaction(g)
print(tr.run(cypher_3depth_forwardback_filtered_search).stats())
tr.run(cypher_build_ids)
tr.run(cypher_build_article_labels)
tr.run(cypher_build_category_labels)
tr.commit()
