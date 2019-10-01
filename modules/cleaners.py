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

    def combinations(self, root_labels):
        root_label_combinations = []
        for i in root_labels:
            for j in root_labels:
                if i < j:
                    root_label_combinations.append([i, j])

        self.root_label_combinations = root_label_combinations

    def run(self, depth):
        self.get_root_labels()
        self.combinations(self.root_labels)

        match_a = "MATCH (z&)-->"
        match_b = "(x)\r"
        pattern_statements = ""
        set_statement = "SET x.keep = 1, y.keep = 1"
        return_statement = "\rRETURN x, y"
        for i in range(depth - 1):
            pattern_statements = pattern_statements + match_a.replace("&", str(i + 1)) + match_b
            return_statement = return_statement + ", z" + str(i + 1)
            set_statement = set_statement + ", z" + str(i + 1) + ".keep = 1"
            match_a = match_a + "()-->"

        cypher_query_1 = """
MATCH (x:root_1:root_2)
MATCH (y:root_node)\r""" + pattern_statements + set_statement + return_statement
        
        cypher_query_set = []
        for i in self.root_label_combinations:
            x = cypher_query_1.replace("root_1", i[0]).replace("root_2", i[1])
            cypher_query_set.append(x)
            print(i)

        print(cypher_query_set[0])
        print(cypher_query_set[1])
        print(cypher_query_set[2])

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

        cypher_query_set = cypher_query_set + [cypher_query_2, cypher_query_3]
        modules.tr_funcs.commit_cypher_query_set(cypher_query_set)
