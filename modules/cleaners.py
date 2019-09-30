import modules.tr_funcs
import numpy


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


class SingleComponentSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "SingleComponentSchemaCleaner"

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
        comps = modules.tr_funcs.commit_cypher_query_numpy(cypher_query).tolist()
        keys = []
        for i in range(len(comps)):
            if comps[i][1] in keys:
                continue
            else:
                keys.append(comps[i][1])
        comps_dict = {}
        for i in range(len(keys)):
            comps_dict[str(keys[i])] = []
        for i in range(len(keys)):
            for j in range(len(comps)):
                if keys[i] == comps[j][1]:
                    comps_dict[keys[i]].append(comps[j][0])

        root_nodes_dict = {}
        for iri in self.root_nodes:
            cypher_query = """
MATCH (x:root_node {iri: \"""" + iri + """\"})--(y:nonroot_node)
RETURN y.iri
            """
            root_nodes_dict[iri] = modules.tr_funcs.commit_cypher_query_numpy(cypher_query).tolist()

        indicator_dict = {}
        for i in keys:
            indicator_dict[i] = 0
            for j in root_nodes_dict:
                for k in comps_dict[i]:
                    if [k] in root_nodes_dict[j]:
                        indicator_dict[i] = indicator_dict[i] + 1
                        break

        vertex_list = []
        for i in indicator_dict:
            if indicator_dict[i] == 1:
                vertex_list = vertex_list + comps_dict[i]

        cypher_query = """
        MATCH (x)
        WHERE x.iri IN """ + str(vertex_list) + """
        DETACH DELETE (x)
        """
        modules.tr_funcs.commit_cypher_query(cypher_query)
