import modules.tr_funcs


class SchemaEnricher:
    def __init__(self):
        self.name = "SchemaEnricher"


class IdsLabelsSchemaEnricher(SchemaEnricher):
    def __init__(self):
        self.name = "IdsLabelsSchemaEnricher"

    def enrich(self):
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

        cypher_query_set = [cypher_build_node_ids, cypher_build_edge_ids, cypher_build_article_labels, cypher_build_category_labels]
        modules.tr_funcs.commit_cypher_query_set(cypher_query_set)
