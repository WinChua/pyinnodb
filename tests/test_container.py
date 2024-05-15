from context import *
import sqlalchemy
import pytest
from testcontainers.mysql import MySqlContainer
from sqlalchemy import text

from pyinnodb.disk_struct.fil import MFil
from pyinnodb.const import get_page_type_name

import tarfile

from docker.models.containers import Container

import logging

logger = logging.getLogger(__name__)

import os
from io import BytesIO

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine

# 创建一个基类
Base = declarative_base()

test_table_name = "users"

# 定义模型类
class User(Base):
    __tablename__ = 'users'  # 表名

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)

# 创建数据库引擎
# engine = create_engine('sqlite:///database.db')
# 
# # 创建表

def open_from_container(container: Container, path: str) -> tarfile.TarFile:
    fileobj = BytesIO()
    bits, stat = container.get_archive(path)
    for chunk in bits:
        fileobj.write(chunk)
    
    fileobj.seek(0)
    tar = tarfile.open(fileobj=fileobj)
    return tar

@pytest.fixture
def setup_mysql_container():
    logger.debug("setup mysql database container ....")

    with MySqlContainer("mysql:8.0.35") as mysql:
        engine = sqlalchemy.create_engine(mysql.get_connection_url())
        logger.debug("mysql is %s", mysql.get_connection_url())
        Base.metadata.create_all(engine)
        logger.debug(type(mysql._container))
        with engine.begin() as connection:
            result = connection.execute(
                text("show variables like '%dir%'")
            )
            result = result.fetchall()
            result = result[0]
            yield mysql, connection

    logger.debug("end test mysql database container ....")

def test_mysql_container(setup_mysql_container):
    mysql, connection = setup_mysql_container
    tar = open_from_container(mysql._container, f"/var/lib/mysql/test/{test_table_name}.ibd")
    f = tar.extractfile(f"{test_table_name}.ibd")
    fil = MFil.parse_stream(f)
    logger.debug("fil is %s", fil)
    logger.debug("page type is %s", get_page_type_name(fil.page_type))
