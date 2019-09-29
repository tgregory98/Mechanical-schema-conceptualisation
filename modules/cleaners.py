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

    def clean(self, depth):
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

    def clean(self):
        self.stats_gen()
        cypher_query = """
CALL algo.unionFind.stream(
"MATCH (x:nonroot_node) RETURN id(x) AS id",
"MATCH (x:nonroot_node)-->(y:nonroot_node) RETURN id(x) AS source, id(y) AS target",
{graph: "cypher"})
YIELD nodeId,setId

RETURN algo.asNode(nodeId).iri AS iri, setId
        """

        print("Components:")
        print(modules.tr_funcs.commit_cypher_query_data(cypher_query))

        for iri in self.root_nodes:
            cypher_query = """
MATCH (x:root_node {iri: \"""" + iri + """\"})--(y:nonroot_node)
RETURN y.iri
            """
            print("\rNeighbours of " + iri + ":")
            print(modules.tr_funcs.commit_cypher_query_data(cypher_query))
