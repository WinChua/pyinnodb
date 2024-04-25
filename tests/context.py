import os
import sys
import logging
from io import BytesIO
import time
import tempfile
import tarfile
import pytest
import sqlalchemy
from sqlalchemy import text
from docker.models.containers import Container
from testcontainers.mysql import MySqlContainer
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import insert
from pyinnodb import const
import sqlalchemy
from sqlalchemy.dialects import mysql as dmysql
from sqlalchemy.schema import CreateTable

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
test_filename = os.path.join(parent_dir, "data", "t2.ibd")
test_filename1 = os.path.join(parent_dir, "data", "t1.ibd")
test_filename2 = test_filename

locate_filename = test_filename1
test_filename = locate_filename

test_table_name = "users"

# docker run --rm -v `pwd`/data:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=test -u 1000:1000 mysql:8.0.35

mysql_conf_test_dir = os.path.dirname(os.path.dirname(__file__)) + "../mysql_conf"

for k in logging.Logger.manager.loggerDict:
    if not k.startswith("pyinnodb"):
        logging.getLogger(k).setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

Base = declarative_base()
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
# all_type_column.extend([f"    {t} = Column(sqlalchemy.types.{t}(10))" for t in all_type_with_length])
logger.info("class define is %s", "\n".join(all_type_column))
exec("\n".join(all_type_column))
AllType = locals().get("AllType")

logger.info(
    "create all_type table is %s",
    CreateTable(AllType.__table__).compile(dialect=dmysql.dialect()),
)

sqlalchemy.types.LargeBinary


# 定义模型类
class User(Base):
    __tablename__ = test_table_name  # 表名

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)


@pytest.fixture
def containerOp():
    logger.debug("setup mysql database container ....")

    try:
        mysql = MySqlContainer("mysql:8.0.35").__enter__()
        # with MySqlContainer("mysql:8.0.35") as mysql:
        engine = sqlalchemy.create_engine(mysql.get_connection_url())
        logger.debug("mysql is %s", mysql.get_connection_url())
        Base.metadata.create_all(engine)
        with engine.connect() as connection:
            result = connection.execute(text("show variables like '%datadir%'"))
            result = result.fetchall()
            result = result[0]
            logger.debug("result is %s", result)
            yield ContainerOp(mysql, engine, connection, str(result[1]))

        if not os.getenv("NO_STOP", None) == "1":
            mysql.stop()

        logger.debug("end test mysql database container ....")
    except Exception as e:
        mysql.stop()
        raise e


class ContainerOp(object):
    def __init__(
        self,
        mysql: MySqlContainer,
        engine: sqlalchemy.Engine,
        connection: sqlalchemy.Connection,
        datadir: str,
    ):
        self.mysql = mysql
        self.engine = engine
        self.connection = connection
        self.datadir = datadir

    def open(self, path: str) -> tarfile.ExFileObject:
        """while this function could work, but i found that the file copy from
        container is not as expect."""
        fileobj = BytesIO()
        bits, stat = self.mysql._container.get_archive(path)
        logger.debug("stat is %s", stat)
        for chunk in bits:
            fileobj.write(chunk)

        fileobj.seek(0)
        tar = tarfile.open(fileobj=fileobj)
        return tar.extractfile(os.path.basename(path))

    def build_ibd(self, *sql):
        for s in sql:
            self.connection.execute(s)
            self.connection.commit()
        time.sleep(3)

    def build_data_path(self, path: str) -> str:
        return self.datadir + path
