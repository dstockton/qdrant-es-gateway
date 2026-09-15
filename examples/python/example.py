from elasticsearch import Elasticsearch

client = Elasticsearch("http://localhost:9200")
client.indices.create(index="docs", mappings={"properties": {"title": {"type": "text"}}}, ignore=[400])
client.index(index="docs", id="unicode-✓", document={"title": "Qdrant application search"})
print(client.search(index="docs", query={"match": {"title": "application search"}}))
