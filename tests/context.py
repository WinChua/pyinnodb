import os
import sys
import logging
from io import BytesIO
from sqlalchemy.orm import declarative_base
from pyinnodb import const
import sqlalchemy
import pathlib
import pytest
from collections import namedtuple

cur_file = pathlib.Path(__file__)

test_data_dir = "tests"

test_mysql5_ibd = cur_file.parent.parent / test_data_dir / "mysql5" / "all_type.ibd"
test_mysql5_frm = test_mysql5_ibd.parent / "all_type.frm"
test_mysql8_ibd = test_mysql5_ibd.parent.parent / "mysql8" / "all_type.ibd"

# docker run --rm -v `pwd`/data:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=test -u 1000:1000 mysql:8.0.35

for k in logging.Logger.manager.loggerDict:
    if not k.startswith("pyinnodb"):
        logging.getLogger(k).setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

Base = declarative_base()

MysqlFile = namedtuple("MysqlFile", "mysql8ibd mysql5ibd mysql5frm")

def download_test_file():
    import requests
    import tarfile
    testfile = "https://github.com/user-attachments/files/16420657/test_data.tgz"
    resp = requests.get(testfile)
    fileobj = BytesIO(resp.content)
    tar = tarfile.open(fileobj=fileobj)
    tar.extractall(test_mysql8_ibd.parent.parent)

@pytest.fixture
def mysqlfile():
    if not test_mysql8_ibd.exists():
        print("download test file")
        download_test_file()

    with open(test_mysql8_ibd, "rb") as f8:
        with open(test_mysql5_frm, "rb") as f5:
            with open(test_mysql5_frm, "rb") as ff5:
                yield MysqlFile(
                    mysql8ibd=f8,
                    mysql5ibd=f5,
                    mysql5frm=ff5,
                )


