from elasticsearch import Elasticsearch
from elasticsearch.client import CatClient
from sqlalchemy import create_engine
from elasticsearch.helpers import bulk

engine = create_engine('clickhouse://user1:123456@localhost:8123/Data')
table = "table1"
data = engine.execute(f"SELECT * FROM {table}").fetchall()
# print(data)
es = Elasticsearch("http://localhost:9200")

def clean_data():
    unique_data = []
    unique_urls = set() 
    for line in data:
        line = dict(line)
        if line['url'] not in unique_urls:
            unique_urls.add(line['url'])
            unique_data.append(line)
    return unique_data

def remove_all_data():
    es.indices.delete(index='my_index')

def info():
    print(es.info().body)

def create_index():
    mappings = {
        "properties": {
        "id": { "type": "integer" },
        "title": { "type": "text" },
        "content": { "type": "text" },
        "url": { "type": "keyword" },
        "published_at": { "type": "date" }
        }
    }
    es.indices.create(index="my_index", mappings=mappings)

def add_data_use_index():
    for line in data:
        doc = {
            'title': line['title'],
            'content': line['content'],
            'url': line['url'],
            'published_at': line['published_at']
        }
        es.index(index="my_index", id=line['id'], document=doc)

def add_data_use_bulk():
    actions = [
        {
            '_index': 'my_index',
            '_id': website['id'],
            '_source': {
                'title': website['title'],
                'content': website['content'],
                'url': website['url'],
                'published_at': website['published_at']
            }
        }
        for website in data
    ]
    bulk(es, actions)

def search(query):
    return es.search(index='my_index', body={
            'query': {
                'multi_match': {
                    'query': query,
                    'fields': ['title^3', 'content','url']
                }
            },
            "sort": [
                {
                    "_score": {
                        "order": "desc"
                    }
                }
            ]
        })['hits']['hits']

def info_indexs():
    cat_client = CatClient(es)
    indices = cat_client.indices()
    print(indices)

if __name__ == "__main__":
    # print(len(data))
    # data = clean_data()
    # remove_all_data()
    # create_index()
    # add_data_use_index()
    info_indexs()
    