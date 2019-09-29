import modules.tr_funcs


class SchemaCleaner:
    def __init__(self):
        self.name = "SchemaCleaner"

    def clean(self, depth):
        modules.tr_funcs.commit_cypher_query_set(self.cypher_query_set_gen(depth))

class LeafSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "LeafSchemaCleaner"

    def cypher_query_set_gen(self, depth):
        cypher_query = """
MATCH (x)
WITH x, size((x)--()) as degree
WHERE degree = 1
DETACH DELETE (x)
        """
        cypher_query_set = []
        for i in range(depth):
            cypher_query_set.append(cypher_query)

        return cypher_query_set


class DisjointParentSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "DisjointParentSchemaCleaner"

