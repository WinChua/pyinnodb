import random

alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root@localhost")


col = 1000
with engine.connect() as conn:
    conn.exec_driver_sql("use test")
    conn.exec_driver_sql("drop table t1")
    conn.exec_driver_sql("create table t1 ( c1 int PRIMARY KEY, name varchar(255) )")
    for i in range(100):
        values = []
        for j in range(col):
            values.append(f"({i*col+j},\"{''.join(random.choices(alphabet,k=10))}\")")
        conn.exec_driver_sql(f"insert into test.t1 value{','.join(values)}")
    conn.commit()
