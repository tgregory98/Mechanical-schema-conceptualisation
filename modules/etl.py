from SPARQLWrapper import SPARQLWrapper
import modules.misc
import logging


class Build:
    def __init__(self, filter_set_edges=[], filter_set_vertices=[]):
        self.name = "Build class"
        self.filter_set_edges = filter_set_edges
        self.filter_set_vertices = filter_set_vertices

    def fetch_node_id(self, page):
        output = page.replace("http://dbpedia.org/resource/Category:", "")
        output = page.replace("http://dbpedia.org/resource/", "")

        return output

    def filter_query_pred_gen(self):
        filter_query_pred = ""
        for i in range(len(self.filter_set_edges)):
            if len(self.filter_set_edges) == 0:
                break
            elif len(self.filter_set_edges) == 1:
                string = "FILTER(regex(?pred£, ££))"
                filter_query_pred = string.replace("££", str(self.filter_set_edges[0]))
            elif i == 0:
                filter_query_pred = "FILTER("
                string = "regex(?pred£, ££)"
                filter_query_pred = filter_query_pred + string.replace("££", str(self.filter_set_edges[i]))
            elif i < len(self.filter_set_edges) - 1:
                string = "||regex(?pred£, ££)"
                filter_query_pred = filter_query_pred + string.replace("££", str(self.filter_set_edges[i]))
            elif i == len(self.filter_set_edges) - 1:
                string = "||regex(?pred£, ££))"
                filter_query_pred = filter_query_pred + string.replace("££", str(self.filter_set_edges[i]))

        return filter_query_pred

    def filter_query_pred_inv_gen(self):
        filter_query_pred_inv = ""
        for i in range(len(self.filter_set_edges)):
            if len(self.filter_set_edges) == 0:
                break
            elif len(self.filter_set_edges) == 1:
                string = "FILTER(regex(?pred_inv£, ££))"
                filter_query_pred_inv = string.replace("££", str(self.filter_set_edges[0]))
            elif i == 0:
                filter_query_pred_inv = "FILTER("
                string = "regex(?pred_inv£, ££)"
                filter_query_pred_inv = filter_query_pred_inv + string.replace("££", str(self.filter_set_edges[i]))
            elif i < len(self.filter_set_edges) - 1:
                string = "||regex(?pred_inv£, ££)"
                filter_query_pred_inv = filter_query_pred_inv + string.replace("££", str(self.filter_set_edges[i]))
            elif i == len(self.filter_set_edges) - 1:
                string = "||regex(?pred_inv£, ££))"
                filter_query_pred_inv = filter_query_pred_inv + string.replace("££", str(self.filter_set_edges[i]))
        
        return filter_query_pred_inv

    def filter_query_vertex_gen(self):
        filter_query_vertex = ""
        for i in range(len(self.filter_set_vertices)):
            if len(self.filter_set_vertices) == 0:
                break
            elif len(self.filter_set_vertices) == 1:
                string = "FILTER(regex(?n£, ££))"
                filter_query_vertex = string.replace("££", str(self.filter_set_vertices[0]))
            elif i == 0:
                filter_query_vertex = "FILTER("
                string = "regex(?n£, ££)"
                filter_query_vertex = filter_query_vertex + string.replace("££", str(self.filter_set_vertices[i]))
            elif i < len(self.filter_set_vertices) - 1:
                string = "||regex(?n£, ££)"
                filter_query_vertex = filter_query_vertex + string.replace("££", str(self.filter_set_vertices[i]))
            elif i == len(self.filter_set_vertices) - 1:
                string = "||regex(?n£, ££))"
                filter_query_vertex = filter_query_vertex + string.replace("££", str(self.filter_set_vertices[i]))
        
        return filter_query_vertex

    def cypher_url_gen(self, sparql_query):
        wrapper = SPARQLWrapper("http://dbpedia.org/sparql")
        wrapper.setQuery(sparql_query)
        wrapper.setReturnFormat("csv")
        query_result = wrapper.query()
        url = query_result.geturl()

        return url

    def run(self, depth):
        sparql_query = self.sparql_query_gen(depth)
        url = self.cypher_url_gen(sparql_query)
        cypher_query = self.cypher_query_gen(depth, url)
        modules.misc.commit_cypher_query(cypher_query)

        cypher_query_combine_nodes = """
MATCH (n1),(n2)
WHERE n1.iri = n2.iri and id(n1) < id(n2)
CALL apoc.refactor.mergeNodes([n1, n2]) YIELD node
RETURN n1, n2
        """
        modules.misc.commit_cypher_query(cypher_query_combine_nodes)
        
        cypher_query_combine_edges = """
MATCH (n1)-[r]->(n2), (n1)-[s]->(n2)
WHERE r.iri = s.iri and id(r) < id(s)
DELETE s
        """
        modules.misc.commit_cypher_query(cypher_query_combine_edges)        


class Pairwise(Build):
    def __init__(self, start_page, end_page, filter_set_edges=[], filter_set_vertices=[]):
        self.name = "Pairwise build between " + start_page + " and " + end_page
        self.start_page = start_page
        self.end_page = end_page
        self.filter_set_edges = filter_set_edges
        self.filter_set_vertices = filter_set_vertices

    def sparql_query_gen(self, depth):
        query_part1 = "\nSELECT "
        for i in range(depth - 1):
            string = "?pred£ ?pred_inv£ ?n£ "
            query_part1 = query_part1 + string.replace("£", str(i + 1))
        final_string = "?pred£ ?pred_inv£\n"
        query_part1 = query_part1 + final_string.replace("£", str(depth))

        filter_query_pred = self.filter_query_pred_gen()
        filter_query_pred_inv = self.filter_query_pred_inv_gen()
        filter_query_vertex = self.filter_query_vertex_gen()
        filter_query_vertex_mid = filter_query_vertex + filter_query_vertex.replace("£", "££")
        filter_query_vertex_mid = filter_query_vertex_mid.replace(")FILTER(", ")&&(").replace("FILTER(", "FILTER((") + ")"

        query_part2_open = """
    WHERE {
        """

        query_part2_a = """
        { {
            <""" + self.start_page + """> ?pred1 ?n1
        } UNION {
            ?n1 ?pred_inv1 <""" + self.start_page + """>
        } } .
        """
        query_part2_b = """
        { {
            """ + filter_query_pred.replace("£", "1") + """
            <""" + self.start_page + """> ?pred1 ?n1
        } UNION {
            """ + filter_query_pred_inv.replace("£", "1") + """
            ?n1 ?pred_inv1 <""" + self.start_page + """>
        } } .
        """
        query_part2_c = """
        { {
            """ + filter_query_vertex.replace("£", "1") + """
            <""" + self.start_page + """> ?pred1 ?n1
        } UNION {
            """ + filter_query_vertex.replace("£", "1") + """
            ?n1 ?pred_inv1 <""" + self.start_page + """>
        } } .
        """
        query_part2_d = """
        { {
            """ + filter_query_pred.replace("£", "1") + filter_query_vertex.replace("£", "1") + """
            <""" + self.start_page + """> ?pred1 ?n1
        } UNION {
            """ + filter_query_pred_inv.replace("£", "1") + filter_query_vertex.replace("£", "1") + """
            ?n1 ?pred_inv1 <""" + self.start_page + """>
        } } .
        """
        for i in range(depth - 2):
            block_a = """
        { {
            ?n£ ?pred££ ?n££
        } UNION {
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            block_b = """
        { {
            """ + filter_query_pred.replace("£", str(i + 2)) + """
            ?n£ ?pred££ ?n££
        } UNION {
            """ + filter_query_pred_inv.replace("£", str(i + 2)) + """
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            block_c = """
        { {
            """ + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } UNION {
            """ + filter_query_vertex_mid + """
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            block_d = """
        { {
            """ + filter_query_pred.replace("£", str(i + 2)) + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } UNION {
            """ + filter_query_pred_inv.replace("£", str(i + 2)) + filter_query_vertex_mid + """
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            query_part2_a = query_part2_a + block_a.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_b = query_part2_b + block_b.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_c = query_part2_c + block_c.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_d = query_part2_d + block_d.replace("££", str(i + 2)).replace("£", str(i + 1))

        final_block_a = """
        { {
            """ + filter_query_pred.replace("£", str(depth)) + """
            ?n£ ?pred££ <""" + self.end_page + """>
        } UNION {
            """ + filter_query_pred_inv.replace("£", str(depth)) + """
            <""" + self.end_page + """> ?pred_inv££ ?n£
        } } .
        """
        final_block_b = """
        { {
            """ + filter_query_pred.replace("£", str(depth)) + """
            ?n£ ?pred££ <""" + self.end_page + """>
        } UNION {
            """ + filter_query_pred_inv.replace("£", str(depth)) + """
            <""" + self.end_page + """> ?pred_inv££ ?n£
        } } .
        """
        final_block_c = """
        { {
            """ + filter_query_vertex + """
            ?n£ ?pred££ <""" + self.end_page + """>
        } UNION {
            """ + filter_query_vertex + """
            <""" + self.end_page + """> ?pred_inv££ ?n£
        } } .
        """
        final_block_d = """
        { {
            """ + filter_query_pred.replace("£", str(depth)) + filter_query_vertex + """
            ?n£ ?pred££ <""" + self.end_page + """>
        } UNION {
            """ + filter_query_pred_inv.replace("£", str(depth)) + filter_query_vertex + """
            <""" + self.end_page + """> ?pred_inv££ ?n£
        } } .
        """
        query_part2_a = query_part2_a + final_block_a.replace("££", str(depth)).replace("£", str(depth - 1))
        query_part2_b = query_part2_b + final_block_b.replace("££", str(depth)).replace("£", str(depth - 1))
        query_part2_c = query_part2_c + final_block_c.replace("££", str(depth)).replace("£", str(depth - 1))
        query_part2_d = query_part2_d + final_block_d.replace("££", str(depth)).replace("£", str(depth - 1))

        query_part2_close = """
    }
        """

        if len(self.filter_set_edges) == 0 and len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_a + query_part2_close
        elif len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_b + query_part2_close
        elif len(self.filter_set_edges) == 0:
            query_part2 = query_part2_open + query_part2_c + query_part2_close
        elif len(self.filter_set_edges) != 0 and len(self.filter_set_vertices) != 0:
            query_part2 = query_part2_open + query_part2_d + query_part2_close

        query = query_part1 + query_part2

        logging.info(query)
        return query

    def cypher_query_gen(self, depth, url):
        query_part1 = "WITH \"" + url + "\" AS url\n\nLOAD CSV WITH HEADERS FROM url AS row\n\n"

        query_part2 = "MERGE (n0:depth_0 {iri: \"" + self.start_page + "\"})\n"
        for i in range(depth - 1):
            string = "MERGE (n£:depth_£ {iri: row.n£})\n"
            query_part2 = query_part2 + string.replace("£", str(i + 1))
        final_string = "MERGE (n£:depth_0 {iri: \"" + self.end_page + "\"})\n"
        query_part2 = query_part2 + final_string.replace("£", str(depth))

        query_part3 = ""
        for i in range(depth):
            block = """
    FOREACH (x IN CASE WHEN row.pred££ IS NULL THEN [] ELSE [1] END | MERGE (n£)-[p:pred {iri: row.pred££}]->(n££))
    FOREACH (x IN CASE WHEN row.pred_inv££ IS NULL THEN [] ELSE [1] END | MERGE (n£)<-[p:pred {iri: row.pred_inv££}]-(n££))
            """
            query_part3 = query_part3 + block.replace("££", str(i + 1)).replace("£", str(i))

        query = query_part1 + query_part2 + query_part3

        logging.info(query)
        return query


class Parent(Build):
    def __init__(self, page, filter_set_edges=[], filter_set_vertices=[]):
        self.name = "Parent build on " + page
        self.page = page
        self.filter_set_edges = filter_set_edges
        self.filter_set_vertices = filter_set_vertices

    def sparql_query_gen(self, depth):
        query_part1 = "\nSELECT "
        for i in range(depth):
            string = "?pred£ ?n£ "
            query_part1 = query_part1 + string.replace("£", str(i + 1))

        filter_query_pred = self.filter_query_pred_gen()
        filter_query_vertex = self.filter_query_vertex_gen()
        filter_query_vertex_mid = filter_query_vertex + filter_query_vertex.replace("£", "££")
        filter_query_vertex_mid = filter_query_vertex_mid.replace(")FILTER(", ")&&(").replace("FILTER(", "FILTER((") + ")"

        query_part2_open = """
    WHERE {
        """

        query_part2_a = """
        {
            <""" + self.page + """> ?pred1 ?n1
        } .
        """
        query_part2_b = """
        {
            """ + filter_query_pred.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } .
        """
        query_part2_c = """
        {
            """ + filter_query_vertex.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } .
        """
        query_part2_d = """
        {
            """ + filter_query_pred.replace("£", "1") + filter_query_vertex.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } .
        """
        for i in range(depth - 1):
            block_a = """
        {
            ?n£ ?pred££ ?n££
        } .
            """
            block_b = """
        {
            """ + filter_query_pred.replace("£", "££") + """
            ?n£ ?pred££ ?n££
        } .
            """
            block_c = """
        {
            """ + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } .
            """
            block_d = """
        {
            """ + filter_query_pred.replace("£", "££") + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } .
            """
            query_part2_a = query_part2_a + block_a.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_b = query_part2_b + block_b.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_c = query_part2_c + block_c.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_d = query_part2_d + block_d.replace("££", str(i + 2)).replace("£", str(i + 1))
            
        query_part2_close = """
    }
        """

        if len(self.filter_set_edges) == 0 and len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_a + query_part2_close
        elif len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_b + query_part2_close
        elif len(self.filter_set_edges) == 0:
            query_part2 = query_part2_open + query_part2_c + query_part2_close
        elif len(self.filter_set_edges) != 0 and len(self.filter_set_vertices) != 0:
            query_part2 = query_part2_open + query_part2_d + query_part2_close

        query = query_part1 + query_part2

        logging.info(query)
        return query

    def cypher_query_gen(self, depth, url):
        query_part1 = "WITH \"" + url + "\" AS url\n\nLOAD CSV WITH HEADERS FROM url AS row\n\n"

        node_id = self.fetch_node_id(self.page)

        query_part2 = "MERGE (n0:depth_0:" + node_id + " {iri: \"" + self.page + "\"})\n"
        for i in range(depth):
            string = "MERGE (n£:depth_£:" + node_id + " {iri: row.n£})\n"
            query_part2 = query_part2 + string.replace("£", str(i + 1))

        query_part3 = ""
        for i in range(depth):
            block = """
    FOREACH (x IN CASE WHEN row.pred££ IS NULL THEN [] ELSE [1] END | MERGE (n£)-[p:pred {iri: row.pred££}]->(n££))
            """
            query_part3 = query_part3 + block.replace("££", str(i + 1)).replace("£", str(i))

        query = query_part1 + query_part2 + query_part3

        logging.info(query)
        return query


class FiniteParent(Build):
    def __init__(self, page, filter_set_edges=[], filter_set_vertices=[]):
        self.name = "FiniteParent build on " + page
        self.page = page
        self.filter_set_edges = filter_set_edges
        self.filter_set_vertices = filter_set_vertices

    def sparql_query_gen(self, depth):
        query_part1 = "\nSELECT "
        for i in range(depth):
            string = "?pred£ ?n£ "
            query_part1 = query_part1 + string.replace("£", str(i + 1))

        filter_query_pred = self.filter_query_pred_gen()
        filter_query_vertex = self.filter_query_vertex_gen()
        filter_query_vertex_mid = filter_query_vertex + filter_query_vertex.replace("£", "££")
        filter_query_vertex_mid = filter_query_vertex_mid.replace(")FILTER(", ")&&(").replace("FILTER(", "FILTER((") + ")"

        query_part2_open = """
    WHERE {
        """

        query_part2_a = """
        {
            <""" + self.page + """> ?pred1 ?n1
        } .
        """
        query_part2_b = """
        {
            """ + filter_query_pred.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } .
        """
        query_part2_c = """
        {
            """ + filter_query_vertex.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } .
        """
        query_part2_d = """
        {
            """ + filter_query_pred.replace("£", "1") + filter_query_vertex.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } .
        """

        temp_a = ""
        temp_b = ""
        temp_c = ""
        temp_d = ""
        final_a = ""
        final_b = ""
        final_c = ""
        final_d = ""

        for i in range(depth - 1):
            block_a = """
        {
            ?n£ ?pred££ ?n££
        } ."""
            block_b = """
        {
            """ + filter_query_pred.replace("£", "££") + """
            ?n£ ?pred££ ?n££
        } ."""
            block_c = """
        {
            """ + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } ."""
            block_d = """
        {
            """ + filter_query_pred.replace("£", "££") + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } ."""

            temp_a = temp_a + block_a.replace("££", str(i + 2)).replace("£", str(i + 1))
            temp_b = temp_b + block_b.replace("££", str(i + 2)).replace("£", str(i + 1))
            temp_c = temp_c + block_c.replace("££", str(i + 2)).replace("£", str(i + 1))
            temp_d = temp_d + block_d.replace("££", str(i + 2)).replace("£", str(i + 1))

            final_a = final_a + """
    OPTIONAL {""" + temp_a + """
    } .
            """
            final_b = final_b + """
    OPTIONAL {""" + temp_b + """
    } .
            """
            final_c = final_c + """
    OPTIONAL {""" + temp_c + """
    } .
            """
            final_d = final_d + """
    OPTIONAL {""" + temp_d + """
    } .
            """

        query_part2_close = """
    }
        """

        if len(self.filter_set_edges) == 0 and len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_a + final_a + query_part2_close
        elif len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_b + final_b + query_part2_close
        elif len(self.filter_set_edges) == 0:
            query_part2 = query_part2_open + query_part2_c + final_c + query_part2_close
        elif len(self.filter_set_edges) != 0 and len(self.filter_set_vertices) != 0:
            query_part2 = query_part2_open + query_part2_d + final_d + query_part2_close

        query = query_part1 + query_part2

        logging.info(query)
        return query

    def cypher_query_gen(self, depth, url):
        query_part1 = "WITH \"" + url + "\" AS url\n\nLOAD CSV WITH HEADERS FROM url AS row\n\n"

        node_id = self.fetch_node_id(self.page)

        query_part2 = """
    FOREACH (x IN CASE WHEN row.pred££ IS NULL THEN [] ELSE [1] END | MERGE (n0:depth_0:""" + node_id + """ {iri: \"""" + self.page + """\"}) MERGE (n££:depth_££:""" + node_id + """ {iri: row.n££}) MERGE (n£)-[p:pred {iri: row.pred££}]->(n££))
            """
        query_part2 = query_part2.replace("££", str(0 + 1)).replace("£", str(0))

        for i in range(depth - 1):
            block = """
    FOREACH (x IN CASE WHEN row.pred££ IS NULL THEN [] ELSE [1] END | MERGE (n£:depth_£:""" + node_id + """ {iri: row.n£}) MERGE (n££:depth_££:""" + node_id + """ {iri: row.n££}) MERGE (n£)-[p:pred {iri: row.pred££}]->(n££))
            """
            query_part2 = query_part2 + block.replace("££", str(i + 2)).replace("£", str(i + 1))

        query = query_part1 + query_part2

        logging.info(query)
        return query


class Populate(Build):
    def __init__(self, page, filter_set_edges=[], filter_set_vertices=[]):
        self.name = "Populate build on " + page
        self.page = page
        self.filter_set_edges = filter_set_edges
        self.filter_set_vertices = filter_set_vertices

    def sparql_query_gen(self, depth):
        query_part1 = "\nSELECT "
        for i in range(depth):
            string = "?pred£ ?pred_inv£ ?n£ "
            query_part1 = query_part1 + string.replace("£", str(i + 1))

        filter_query_pred = self.filter_query_pred_gen()
        filter_query_pred_inv = self.filter_query_pred_inv_gen()
        filter_query_vertex = self.filter_query_vertex_gen()
        filter_query_vertex_mid = filter_query_vertex + filter_query_vertex.replace("£", "££")
        filter_query_vertex_mid = filter_query_vertex_mid.replace(")FILTER(", ")&&(").replace("FILTER(", "FILTER((") + ")"

        query_part2_open = """
    WHERE {
        """

        query_part2_a = """
        { {
            <""" + self.page + """> ?pred1 ?n1
        } UNION {
            ?n1 ?pred_inv1 <""" + self.page + """>
        } } .
        """
        query_part2_b = """
        { {
            """ + filter_query_pred.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } UNION {
            """ + filter_query_pred_inv.replace("£", "1") + """
            ?n1 ?pred_inv1 <""" + self.page + """>
        } } .
        """
        query_part2_c = """
        { {
            """ + filter_query_vertex.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } UNION {
            """ + filter_query_vertex.replace("£", "1") + """
            ?n1 ?pred_inv1 <""" + self.page + """>
        } } .
        """
        query_part2_d = """
        { {
            """ + filter_query_pred.replace("£", "1") + filter_query_vertex.replace("£", "1") + """
            <""" + self.page + """> ?pred1 ?n1
        } UNION {
            """ + filter_query_pred_inv.replace("£", "1") + filter_query_vertex.replace("£", "1") + """
            ?n1 ?pred_inv1 <""" + self.page + """>
        } } .
        """
        for i in range(depth - 1):
            block_a = """
        { {
            ?n£ ?pred££ ?n££
        } UNION {
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            block_b = """
        { {
            """ + filter_query_pred.replace("£", str(i + 2)) + """
            ?n£ ?pred££ ?n££
        } UNION {
            """ + filter_query_pred_inv.replace("£", str(i + 2)) + """
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            block_c = """
        { {
            """ + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } UNION {
            """ + filter_query_vertex_mid + """
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            block_d = """
        { {
            """ + filter_query_pred.replace("£", str(i + 2)) + filter_query_vertex_mid + """
            ?n£ ?pred££ ?n££
        } UNION {
            """ + filter_query_pred_inv.replace("£", str(i + 2)) + filter_query_vertex_mid + """
            ?n££ ?pred_inv££ ?n£
        } } .
            """
            query_part2_a = query_part2_a + block_a.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_b = query_part2_b + block_b.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_c = query_part2_c + block_c.replace("££", str(i + 2)).replace("£", str(i + 1))
            query_part2_d = query_part2_d + block_d.replace("££", str(i + 2)).replace("£", str(i + 1))

        query_part2_close = """
    }
        """

        if len(self.filter_set_edges) == 0 and len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_a + query_part2_close
        elif len(self.filter_set_vertices) == 0:
            query_part2 = query_part2_open + query_part2_b + query_part2_close
        elif len(self.filter_set_edges) == 0:
            query_part2 = query_part2_open + query_part2_c + query_part2_close
        elif len(self.filter_set_edges) != 0 and len(self.filter_set_vertices) != 0:
            query_part2 = query_part2_open + query_part2_d + query_part2_close

        query = query_part1 + query_part2

        logging.info(query)
        return query

    def cypher_query_gen(self, depth, url):
        query_part1 = "WITH \"" + url + "\" AS url\n\nLOAD CSV WITH HEADERS FROM url AS row\n\n"

        node_id = self.fetch_node_id(self.page)

        query_part2 = "MERGE (n0:depth_0:" + node_id + " {iri: \"" + self.page + "\"})\n"
        for i in range(depth):
            string = "MERGE (n£:depth_£:" + node_id + " {iri: row.n£})\n"
            query_part2 = query_part2 + string.replace("£", str(i + 1))

        query_part3 = ""
        for i in range(depth):
            block = """
    FOREACH (x IN CASE WHEN row.pred££ IS NULL THEN [] ELSE [1] END | MERGE (n£)-[p:pred {iri: row.pred££}]->(n££))
    FOREACH (x IN CASE WHEN row.pred_inv££ IS NULL THEN [] ELSE [1] END | MERGE (n£)<-[p:pred {iri: row.pred_inv££}]-(n££))
            """
            query_part3 = query_part3 + block.replace("££", str(i + 1)).replace("£", str(i))

        query = query_part1 + query_part2 + query_part3

        logging.info(query)
        return query


class Clean:
    def __init__(self):
        self.name = "Clean class"


class Leaf(Clean):
    def __init__(self):
        self.name = "Leaf clean"

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
        modules.misc.commit_cypher_query_set(cypher_query_set)


class DisjointParent(Clean):
    def __init__(self):
        self.name = "DisjointParent clean"

    def get_root_labels(self):
        cypher_query = """
MATCH (x:depth_0)
RETURN DISTINCT labels(x)
        """
        output = modules.misc.commit_cypher_query_numpy(cypher_query).tolist()
        self.root_labels = []
        for i in output:
            i[0].remove("depth_0")
            self.root_labels.append(i[0][0])
        logging.info(self.root_labels)

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
                cypher_query_1_set.append(cypher_query_1c)
        
        cypher_query_set = []
        for i in self.root_label_combinations:
            for j in cypher_query_1_set:
                x = j.replace("root_1", i[0]).replace("root_2", i[1])
                cypher_query_set.append(x)

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
        logging.info(cypher_query_set)
        modules.misc.commit_cypher_query_set(cypher_query_set)

