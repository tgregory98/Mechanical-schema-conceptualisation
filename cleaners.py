class SchemaCleaner:
    def __init__(self, essential_nodes):
        self.essential_nodes = essential_nodes


class LeafSchemaCleaner(SchemaCleaner):
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
