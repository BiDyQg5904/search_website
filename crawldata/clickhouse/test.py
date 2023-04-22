import clickhouse

orm = clickhouse.ClickHouseORM(database="Data", table_name='table1')

data = {
    'id': 181,
    'title': 'quqq619q98',
    'content': 'wioeywiuye',
    'url': 'url',
    'published_at': 1681897780000,
    'sign': 1
}

print(orm.insert('table1', data))