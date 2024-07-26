import sqlalchemy
from sqlalchemy.dialects import mysql as dmysql
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column
from sqlalchemy import text, insert
import time


with open(".deploy_mysqld") as f:
    url = f.readline().strip()

all_type_with_length = ["VARBINARY", "CHAR", "NCHAR", "NVARCHAR", "VARCHAR"]

all_clob_type = [
    "TINYTEXT",  # L + 1 bytes, where L < 2**8  (255)
    "TEXT",  # L + 2 bytes, where L < 2**16 (64 K)
    "MEDIUMTEXT",  # L + 3 bytes, where L < 2**24 (16 MB)
    "LONGTEXT",  # L + 4 bytes, where L < 2**32 (4 GB)
]
all_type = [
    t
    for t in dir(dmysql.types)
    if t.isupper() and t not in ["ARRAY", "NULLTYPE", "STRINGTYPE", "DECIMAL"] and "CHAR" not in t
]
Base = declarative_base()
all_type_column = [
    "class AllType(Base):",
    "    __tablename__ = 'all_type'",
    "    id = Column(dmysql.INTEGER, primary_key=True)",
]
all_type_column.extend(
    [f"    {t} = Column(dmysql.types.{t}, comment='COMMENT FOR {t}')" for t in all_type]
)
all_type_column.append("    ENUM = Column(dmysql.ENUM('hello', 'world', 'a'))")
all_type_column.append("    SET = Column(dmysql.SET('a', 'b', 'c'))")
all_type_column.append(
    "    DECIMAL = Column(dmysql.types.DECIMAL(10, 2))"
)
all_type_column.append(
    "    CHAR = Column(dmysql.types.CHAR(20), nullable=True)"
)
all_type_column.append(
    "    VARBINARY = Column(dmysql.VARBINARY(203))"
)
all_type_column.append(
    "    int_def_col = Column(dmysql.types.BIGINT, server_default=text('42'))"
)
all_type_column.append(
    "    str_def_col = Column(dmysql.types.VARCHAR(255), server_default='world')"
)
exec("\n".join(all_type_column))
AllType = locals().get("AllType")
engine = sqlalchemy.create_engine(url)

with engine.connect() as conn:
    conn.exec_driver_sql("use test")
    conn.exec_driver_sql("drop table if exists all_type")
    conn.commit()

Base.metadata.create_all(engine)

with sessionmaker(bind=engine)() as session:
    test_data = AllType(BIGINT=98283201,
        BIT = 1,
        DATETIME='2024-01-01 09:00:01',
        DOUBLE = 3.1415926,
        FLOAT = 6.189,
        INTEGER = 8621,
        DECIMAL=910.79,
        LONGBLOB=text("repeat('x', 100)"),
        LONGTEXT = text("repeat('g', 3)"),
        MEDIUMBLOB = text("NULL"),
        MEDIUMINT = 999999,
        MEDIUMTEXT = text("NULL"),
        NUMERIC = 10.9,
        REAL = 1092.892,
        SMALLINT = 981,
        TEXT = "TEXT",
        TIME = '03:04:00',
        TIMESTAMP = "2024-07-24 09:05:28",
        YEAR = 2024,
        ENUM = "a",
        SET  = "a,b,c",
        TINYBLOB = b"TINYBLOB",
        TINYINT = 99,
        TINYTEXT = "TINYTEXT",
        CHAR = "09283012",
        VARBINARY = b"VARBINARY",
    )
    test_data2 = AllType(BIGINT=98283201,
        BIT = 1,
        DATETIME='2024-01-01 09:00:01',
        DOUBLE = 3.1415926,
        FLOAT = 6.189,
        INTEGER = 8621,
        DECIMAL=910.79,
        LONGBLOB=text("repeat('x', 100)"),
        LONGTEXT = text("repeat('g', 3)"),
        MEDIUMBLOB = text("NULL"),
        MEDIUMINT = 999999,
        MEDIUMTEXT = text("NULL"),
        NUMERIC = 10.9,
        REAL = 1092.892,
        SMALLINT = 981,
        TEXT = "TEXT",
        TIME = '03:04:00',
        TIMESTAMP = "2024-07-24 09:05:28",
        YEAR = 2024,
        ENUM = "a",
        SET  = "a,b,c",
        TINYBLOB = b"TINYBLOB",
        TINYINT = 99,
        TINYTEXT = "TINYTEXT",
        CHAR = text("NULL"),
        VARBINARY = b"VARBINARY",
    )
    session.add(test_data)
    session.add(test_data2)
    session.commit()
