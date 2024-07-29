import os
import tarfile

import pytest
import sqlalchemy

from io import BytesIO

from testcontainers.mysql import MySqlContainer

from .context import *



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

    def build_ibd(self, *sql, nosleep=None):
        for s in sql:
            self.connection.execute(s)
            self.connection.commit()
        if nosleep is None:
            time.sleep(5)

    def build_data_path(self, path: str) -> str:
        return self.datadir + path


@pytest.fixture
def containerOp():
    logger.debug("setup mysql database container ....")

    try:
        mContainer = MySqlContainer("mysql:8.0.35")
        mContainer.with_volume_mapping(os.getcwd() + "/" + "datadir_test", "/var/lib/mysql", "rw")
        mysql = mContainer.__enter__()
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
        raise e
