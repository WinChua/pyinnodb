import os
import sys
import logging
from sqlalchemy.orm import declarative_base
from pyinnodb import const
import sqlalchemy
import pathlib

cur_file = pathlib.Path(__file__)

test_mysql5_ibd = cur_file.parent.parent / "data" / "mysql5" / "all_type.ibd"
test_mysql5_frm = test_mysql5_ibd.parent / "all_type.frm"
test_mysql8_ibd = test_mysql5_ibd.parent.parent / "mysql8" / "all_type.ibd"

# docker run --rm -v `pwd`/data:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=test -u 1000:1000 mysql:8.0.35

for k in logging.Logger.manager.loggerDict:
    if not k.startswith("pyinnodb"):
        logging.getLogger(k).setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

Base = declarative_base()



