from testcontainers.mysql import MySqlContainer

import sqlalchemy

mysql = MySqlContainer("mysql:8.0.35").__enter__()
print(mysql.get_connection_url())

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine

# 创建一个基类
Base = declarative_base()

test_table_name = "users"

# 定义模型类
class User(Base):
    __tablename__ = test_table_name # 表名

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)

engine = sqlalchemy.create_engine(mysql.get_connection_url())
Base.metadata.create_all(engine)

from sqlalchemy import insert

with engine.begin() as connection:
    connection.execute(
            insert(User).values(name='WinChua', age=30))

from tests.context import open_from_container

tar = open_from_container(mysql._container, "/var/lib/mysql/test/users.ibd").extractfile("users.ibd")
with open("hello.ibd", "wb") as f:
    f.write(tar.read())
