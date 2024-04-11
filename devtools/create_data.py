import sqlalchemy

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine

Base = declarative_base()
test_table_name = "test_users"

class User(Base):
    __tablename__ = test_table_name

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)

# mysql -u root -h 127.0.0.1 -P19203 -ptest
engine = create_engine("mysql+pymysql://root:test@127.0.0.1:19203/test")
Base.metadata.create_all(engine)
