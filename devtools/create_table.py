import sqlalchemy
from sqlalchemy.dialects import mysql as dmysql
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy import text

with open(".deploy_mysqld") as f:
    url = f.readline().strip()

all_type = [
    "BIGINT",
    "BINARY",
    "BLOB",
    "BOOLEAN",
    # 'CLOB',
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "DOUBLE_PRECISION",
    "FLOAT",
    "INT",
    "INTEGER",
    "JSON",
    # 'NULLTYPE',
    "NUMERIC",
    "REAL",
    "SMALLINT",
    # 'STRINGTYPE',
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "ENUM",
    ## 'UUID',
]

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
    if t.isupper() and t not in ["ARRAY", "NULLTYPE", "STRINGTYPE"] and "CHAR" not in t
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
all_type_column.append(
    "    int_def_col = Column(dmysql.types.BIGINT, server_default=text('42'))"
)
all_type_column.append(
    "    str_def_col = Column(dmysql.types.VARCHAR(255), server_default='world')"
)
exec("\n".join(all_type_column))
AllType = locals().get("AllType")
engine = sqlalchemy.create_engine(url)
Base.metadata.create_all(engine)
