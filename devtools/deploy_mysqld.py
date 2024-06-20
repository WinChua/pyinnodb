import os
import sys
import shutil

if len(sys.argv) > 1 and sys.argv[1] == "clean":
    if os.path.exists("datadir"):
        shutil.rmtree("datadir")
    if os.path.exists(".deploy_mysqld"):
        with open(".deploy_mysqld") as f:
            data = f.readlines()
            os.system(f"docker stop {data[1].strip()}")
        os.remove(".deploy_mysqld")
    
else:
    if os.path.exists(".deploy_mysqld"):
        print("a mysqld container has been deployd")
        with open(".deploy_mysqld", "r") as f:
            print(f.read())
            sys.exit(0)

    from testcontainers.mysql import MySqlContainer
    from docker.models.containers import Container

    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    mContainer = MySqlContainer("mysql:8.0.35")
    mContainer.with_volume_mapping(os.getcwd() + "/datadir", "/var/lib/mysql", "rw")
    os.makedirs(os.getcwd() + "/datadir")
    mContainer.with_kwargs(remove=True, user=os.getuid(), userns_mode="host")
    mysql = mContainer.start()
    with open(".deploy_mysqld", "w") as f:
        f.write("\n".join((
            mysql.get_connection_url(),
            f"{mysql._container.short_id}",
            f"mysql -h 127.0.0.1 -P{mysql.get_exposed_port(mysql.port)} -u{mysql.username} -p{mysql.password}",
        )))
