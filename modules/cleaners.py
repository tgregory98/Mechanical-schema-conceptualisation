import modules.tr_funcs
import random
import string


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
YIELD nodeId, setId

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


class DisjointParentSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "DisjointParentSchemaCleaner"

    def clean(self, depth):
        self.stats_gen()


class OutdatedDisjointParentSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "OutdatedDisjointParentSchemaCleaner"

    def clean(self, depth):
        self.stats_gen()

        size = int((depth - 2) * (depth - 1) / 2)
        self.node_tags = self.random_string_set_gen(size, 3)
        self.RETURN_statement = "RETURN "

        cypher_query = ""

        for i in self.combinations(self.root_nodes):
            cypher_query = cypher_query + self.MATCH_query_gen(depth, i[0], i[1])

        cypher_query = cypher_query + "\r" + self.RETURN_statement[:-2]
        print(cypher_query)
        modules.tr_funcs.commit_cypher_query(cypher_query)

    def random_string_set_gen(self, size, item_length):
        letters = string.ascii_lowercase
        output_set = []
        while len(output_set) < size:
            item = "".join(random.choice(letters) for i in range(item_length))
            if item in output_set:
                continue
            else:
                output_set.append(item)

        return output_set

    def combinations(self, input_set):
        output_set = []
        for i in input_set:
            for j in input_set:
                if i == j:
                    continue
                elif i < j:
                    output_set.append([i, j])

        return output_set

    def MATCH_query_gen(self, depth, url1, url2):
        cypher_query = ""
        for i in range(2, depth):
            pattern = "MATCH "
            for j in range(i + 1):
                if j == 0:
                    pattern = pattern + "($0:root_node {iri: \"" + url1 + "\"})&1"
                elif j < i:
                    pattern = pattern + "($" + str(j) + ":nonroot_node)&" + str(j + 1)
                elif j == i:
                    pattern = pattern + "($" + str(j) + ":root_node {iri: \"" + url2 + "\"})"
            # print("\rpattern: " + str(pattern))
            for j in range(1, i):
                # print("\rpivot vertex: " + str(j))
                temp_pattern = pattern
                for k in range(0, j):
                    # print("    right: " + str(k + 1))
                    temp_pattern = temp_pattern.replace("&" + str(k + 1), "-->")
                for k in range(j, i):
                    # print("    left: " + str(k + 1))
                    temp_pattern = temp_pattern.replace("&" + str(k + 1), "<--")
                # print(temp_pattern)
                cypher_query = cypher_query + temp_pattern.replace("$", self.node_tags[0]) + "\r"
                for k in range(i + 1):
                    self.RETURN_statement = self.RETURN_statement + self.node_tags[0] + str(k) + ", "
                self.node_tags.pop(0)
        # print("\rcypher_query:\r" + str(cypher_query))

        return cypher_query
