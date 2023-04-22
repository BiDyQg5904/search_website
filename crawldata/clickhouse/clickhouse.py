from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, ForeignKey, DDL
from clickhouse_sqlalchemy import types, engines, make_session, get_declarative_base

class ClickHouseORM:
    def _create_table(self, table_name):
        metadata = MetaData(bind=self.engine)
        Base = get_declarative_base(metadata=metadata)
        class Table1(Base):
            __tablename__ = table_name
            id = Column(types.UInt16, primary_key=True)
            title = Column(types.String)
            content = Column(types.String)
            url = Column(types.String)
            published_at = Column(types.DateTime64)
            sign = Column(types.Int8)
            __table_args__ = (
                engines.CollapsingMergeTree(
                    sign_col=sign,
                    partition_by=id,
                    order_by=(id)
                ),
            )
        Table1.__table__.create()
    
    def __init__(self, host='localhost', username='user1', password='123456', database='default', table_name='table1'):
        self.engine = create_engine(f'clickhouse://{username}:{password}@{host}:8123/default')
        self.engine.execute(DDL(f'CREATE DATABASE IF NOT EXISTS {database}'))
        self.engine = create_engine(f'clickhouse://{username}:{password}@{host}:8123/{database}')
        tables_list = self.engine.execute("SHOW TABLES").fetchall()
        # print(tables_list)
        if table_name not in [x[0] for x in tables_list]:
            self._create_table(table_name)
    
    # def drop_table(self, table_name: str) -> bool:
    #     if table_name == 'table1':
    #         self.engine.execute(f"DROP TABLE {table_name}")
    #         return True
    #     return False
            
    def insert(self, table_name: str, data: dict) -> bool:
        if table_name == "table1":
            try:
                query = """
                    INSERT INTO table1 (id, title, content, url, published_at, sign) 
                    VALUES (%(id)s, %(title)s, %(content)s, %(url)s, %(published_at)s, %(sign)s)
                """
                self.engine.execute(query, data)
                return True
            except Exception as e:
                print(e)
                return False
        return False

    # def drop_database(self, database_name: str) -> bool:
    #     try:    
    #         self.engine.execute(DDL(f"DROP DATABASE IF EXISTS {database_name}"));
    #         return True
    #     except Exception as e:
    #         print(f"Failed to drop database {database_name}: {str(e)}")
    #         return False