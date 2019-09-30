import modules.tr_funcs


class SchemaCleaner:
    def __init__(self):
        self.name = "SchemaCleaner"

    def stats_gen(self):
        cypher_query_root_nodes = """
MATCH (x:root_node)
RETURN x.iri
        """
        root_nodes = modules.tr_funcs.commit_cypher_query_data(cypher_query_root_nodes)
        self.root_nodes = [root_nodes[i]["x.iri"] for i in range(len(root_nodes))]


class LeafSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "LeafSchemaCleaner"

    def run(self, depth):
        cypher_query = """
MATCH (x)
WITH x, size((x)--()) as degree
WHERE degree = 1
DETACH DELETE (x)
        """
        cypher_query_set = []
        for i in range(depth):
            cypher_query_set.append(cypher_query)
        modules.tr_funcs.commit_cypher_query_set(cypher_query_set)


class DisjointParentSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "DisjointParentSchemaCleaner"

    def run(self):
        cypher_query = ""
