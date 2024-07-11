import sys
import random
import sqlalchemy

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine, insert

Base = declarative_base()
test_table_name = "test_users"


class User(Base):
    __tablename__ = test_table_name

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)

with open(".deploy_mysqld") as f:
    url = f.readline().strip()

# mysql -u root -h 127.0.0.1 -P19203 -ptest
print(url)
engine = create_engine(url)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

if len(sys.argv) > 1 and sys.argv[1] == "random":
    ids = iter(random.sample(range(0, 2*1000*1000), 1000*1000))
    for i in range(1000):
        session.bulk_insert_mappings(User, [{"id": next(ids), "name":"Hello", "age": 10} for j in range(1000)])
elif len(sys.argv) > 1 and sys.argv[1] == "drop":
    User.__table__.drop(bind=engine)
else:
    for i in range(1000):
        session.bulk_insert_mappings(User, [{"name":"Hello", "age": 10} for i in range(1000)])

session.commit()
