from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, ForeignKey
from clickhouse_sqlalchemy import types, engines

from elasticsearch import Elasticsearch

engine = create_engine('clickhouse://user1:123456@localhost:8123/Data')
table = "table1"
data = engine.execute(f"SELECT * FROM {table}").fetchall()

es = Elasticsearch("http://localhost:9200")

for line in data:
    line = dict(line)
    doc = {
        'title': line['title'],
        'content': line['content'],
        'url': line['url'],
        'published_at': line['published_at']
    }
    es.index(index="my_index", id=line['id'], document=doc)