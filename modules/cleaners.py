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
MATCH (x:depth_0)
RETURN DISTINCT labels(x)
        """
        output = modules.tr_funcs.commit_cypher_query_numpy(cypher_query).tolist()
        self.root_labels = []
        for i in output:
            i[0].remove("depth_0")
            self.root_labels.append(i[0][0])
        print(self.root_labels)

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
        
        cypher_query_1_set = []
        cypher_query_1a = """
MATCH (x:depth_0)
MATCH (y:root_1:root_2)
SET x.keep = 1, y.keep = 1
        """
        cypher_query_1_set.append(cypher_query_1a)
        
        match_a = "MATCH (x:depth_0)-->(n1)-->"
        match_b = "(y:root_1:root_2)"
        pattern_statement = ""
        set_statement = "SET n1.keep = 1"
        if depth >= 2:
            cypher_query_1b = match_a + match_b + "\n" + set_statement + "\n"
            cypher_query_1_set.append(cypher_query_1b)
            
            for i in range(depth - 2):
                pattern_statement = ""
                match_a = match_a + "(n&)-->".replace("&", str(i + 2))
                pattern_statement = pattern_statement + match_a + match_b
                set_statement = set_statement + ", n" + str(i + 2) + ".keep = 1"

                cypher_query_1c = pattern_statement + "\n" + set_statement
                print(cypher_query_1c)
                cypher_query_1_set.append(cypher_query_1c)
        
        cypher_query_set = []
        for i in self.root_label_combinations:
            for j in cypher_query_1_set:
                x = j.replace("root_1", i[0]).replace("root_2", i[1])
                cypher_query_set.append(x)
        print(cypher_query_set)
        cypher_query_2 = """
MATCH (x)
WHERE x.keep IS NULL
DETACH DELETE x
        """

        cypher_query_3 = """
MATCH (x)
SET x.keep = NULL
        """

        cypher_query_set = cypher_query_set + [cypher_query_2, cypher_query_3]
        modules.tr_funcs.commit_cypher_query_set(cypher_query_set)


class NodeSchemaCleaner(SchemaCleaner):
    def __init__(self):
        self.name = "NodeSchemaCleaner"

    def run(self):
        modules.tr_funcs.commit_cypher_query("""
MATCH (x)
WHERE x.iri STARTS WITH "http://www.w3.org/2000/01/rdf-schema#"
DETACH DELETE (x)
        """)
