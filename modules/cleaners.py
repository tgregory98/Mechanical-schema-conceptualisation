import modules.tr_funcs


class SchemaCleaner:
    def __init__(self):
        self.name = "SchemaCleaner"


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

    def get_root_labels(self):
        cypher_query = """
MATCH (x:root_node)
RETURN DISTINCT labels(x)
        """
        output = modules.tr_funcs.commit_cypher_query_numpy(cypher_query).tolist()
        self.root_labels = [output[i][0][1] for i in range(len(output))]

    def run(self, depth):
        self.get_root_labels()
        # Currently only supports two vertices, and a depth of 3
        cypher_query_1 = """
MATCH (x:""" + self.root_labels[0] + """:""" + self.root_labels[1] + """)
MATCH (y:root_node)
MATCH (a)-->(x)
MATCH (b)-->()-->(x)
SET x.keep = 1, y.keep = 1, a.keep = 1, b.keep = 1
RETURN x, y, a, b
        """
        cypher_query_2 = """
MATCH (x)
WHERE x.keep IS NULL
DETACH DELETE x
        """
        cypher_query_3 = """
MATCH (x)
SET x.keep = NULL
RETURN x
        """
        cypher_query_set = [cypher_query_1, cypher_query_2, cypher_query_3]
        modules.tr_funcs.commit_cypher_query_set(cypher_query_set)
