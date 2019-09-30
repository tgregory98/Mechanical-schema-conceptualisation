from py2neo import Graph, Transaction


g = Graph("bolt://localhost:7687", auth=("neo4j", "cayley"))


def commit_cypher_query(cypher_query):
    tr = Transaction(g)
    tr.run(cypher_query)
    tr.commit()


def commit_cypher_query_set(cypher_query_set):
    tr = Transaction(g)
    for cypher_query in cypher_query_set:
        tr.run(cypher_query)
    tr.commit()


def commit_cypher_query_data(cypher_query):
    tr = Transaction(g)
    dictionary = tr.run(cypher_query).data()

    return dictionary


def commit_cypher_query_numpy(cypher_query):
    tr = Transaction(g)
    array = tr.run(cypher_query).to_ndarray()

    return array
