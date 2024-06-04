import os
import sys


if os.path.exists(".deploy_mysqld"):
    print("a mysqld container has been deployd")
    with open(".deploy_mysqld", "r") as f:
        print(f.read())
        sys.exit(0)

from testcontainers.mysql import MySqlContainer
from docker.models.containers import Container

os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
mContainer = MySqlContainer("mysql:8.0.35")
mContainer.with_volume_mapping(os.getcwd() + "/" + "datadir", "/var/lib/mysql", "rw")
mysql = mContainer.start()
with open(".deploy_mysqld", "w") as f:
    f.write(mysql.get_connection_url())
