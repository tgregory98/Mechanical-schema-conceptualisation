from SPARQLWrapper import SPARQLWrapper


class SchemaCleaner:

MATCH (k)
WITH k, size((k)-[:TYPE]->()) as degree
WHERE k.Value='30 ' AND degree > 1
MATCH (k)-[r:TYPE]->(n:ABC)
RETURN n,r,k,degree;