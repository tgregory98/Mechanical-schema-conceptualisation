# import sys
# print(sys.executable)

import datetime
from SPARQLWrapper import SPARQLWrapper
from py2neo import Database, Graph, Transaction, Cursor

db = Database("bolt://localhost:7687", auth=("neo4j", "cayley"))
g = Graph("bolt://localhost:7687", auth=("neo4j", "cayley"))

class PairwiseSchemaBuilder:

    def __init__(self, start_page, end_page):
        self.start_page = start_page
        self.end_page = end_page

    def sparql_query_gen(self, depth):
        query_part1 = "\rSELECT "
        for i in range(depth-1):
            i = i + 1
            string = "?pred& ?pred_inv& ?n& "
            query_part1 = query_part1 + string.replace("&", str(i))
        final_string = "?pred& ?pred_inv&\r"
        query_part1 = query_part1 + final_string.replace("&", str(depth))
        
        query_part2 = """
    WHERE {
        { {
            FILTER(regex(?pred1, dct:subject)
            ||regex(?pred1, skos:broader))
            <""" + self.start_page + """> ?pred1 ?n1
        } UNION {
            FILTER(regex(?pred_inv1, dct:subject)
            ||regex(?pred_inv1, skos:broader))
            ?n1 ?pred_inv1 <""" + self.start_page + """>
        } } .
        """
        for i in range(depth-2):
            i = i + 1
            block = """
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
            query_part2 = query_part2 + block.replace("&&", str(i+1)).replace("&", str(i))

        final_block = """
        { {
            FILTER(regex(?pred&&, dct:subject)
            ||regex(?pred&&, skos:broader))
            ?n& ?pred&& <""" + self.end_page + """>
        } UNION {
            FILTER(regex(?pred_inv&&, dct:subject)
            ||regex(?pred_inv&&, skos:broader))
            <""" + self.end_page + """> ?pred_inv&& ?n&
        } } .
    }
        """
        query_part2 = query_part2 + final_block.replace("&&", str(depth)).replace("&", str(depth-1))

        query = query_part1 + query_part2
        return query

    def cypher_url_gen(self, sparql_query):
        wrapper = SPARQLWrapper("http://dbpedia.org/sparql")
        wrapper.setQuery(sparql_query)
        wrapper.setReturnFormat("csv")
        query_result = wrapper.query()
        url = query_result.geturl()

        return url

    def cypher_query_gen(self, depth, url):
        query_part1 = "WITH \"" + url + "\" AS url\r\rLOAD CSV WITH HEADERS FROM url AS row\r\r"

        query_part2 = "MERGE (n0:page:start_page {iri: \"" + self.start_page + "\"})\r"
        for i in range(depth-1):
            i = i + 1
            string = "MERGE (n&:page {iri: row.n&})\r"
            query_part2 = query_part2 + string.replace("&", str(i))
        final_string = "MERGE (n&:page:end_page {iri: \"" + self.end_page + "\"})\r"
        query_part2 = query_part2 + final_string.replace("&", str(depth))

        query_part3 = ""
        for i in range(depth):
            block = """
    FOREACH (x IN CASE WHEN row.pred&& IS NULL THEN [] ELSE [1] END | MERGE (n&)-[p:pred {iri: row.pred&&}]->(n&&))
    FOREACH (x IN CASE WHEN row.pred_inv&& IS NULL THEN [] ELSE [1] END | MERGE (n&)<-[p:pred {iri: row.pred_inv&&}]-(n&&))
            """
            query_part3 = query_part3 + block.replace("&&", str(i+1)).replace("&", str(i))

        query_part4 = "\rRETURN "
        for i in range(depth):
            string = "n&, "
            query_part4 = query_part4 + string.replace("&", str(i))
        final_string = "n&"
        query_part4 = query_part4 + final_string.replace("&", str(depth))

        query = query_part1 + query_part2 + query_part3 + query_part4
        return query

    def cypher_query_gen_from_depth(self, depth):
        sparql_query = self.sparql_query_gen(depth)
        url = self.cypher_url_gen(sparql_query)
        cypher_initial = self.cypher_query_gen(depth, url)

        return cypher_initial

    def build_at_depth(self, depth):
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
        
        cypher_query = self.cypher_query_gen_from_depth(depth)
        cypher_set = [cypher_query, cypher_build_node_ids, cypher_build_edge_ids, cypher_build_article_labels, cypher_build_category_labels]
        tr = Transaction(g)
        for cypher in cypher_set:
            tr.run(cypher)
        tr.commit()
        print("Depth " + str(depth) + " build complete at " + str(datetime.datetime.now().time()) + ".")




sch = PairwiseSchemaBuilder("http://dbpedia.org/resource/Television", "http://dbpedia.org/resource/Netflix")
sch.build_at_depth(2)
sch.build_at_depth(3)
sch.build_at_depth(4)
