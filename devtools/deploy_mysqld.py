import os
import shutil
import click
import json
from pprint import pprint

from dataclasses import dataclass, asdict
from testcontainers.mysql import MySqlContainer
from testcontainers.core.config import testcontainers_config as c

from sqlalchemy import create_engine


from pyinnodb import const
from pyinnodb import disk_struct
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.sdi.table import Table

c.ryuk_disabled = True


@click.group()
def main():
    pass


@main.command(name="list")
def tlist():
    data = load_deploy()
    pprint(data)


@main.command()
@click.option("--version", type=click.STRING)
def clean(version):
    if version == "":
        print("you should specify a version")
        return
    data = load_deploy()
    deploy = data.get(version, None)
    if deploy is None:
        print(f"{version} not deploy now, only these version had beed deploy")
        print(f"\t{','.join(k for k in data)}")
        return
    os.system(f"docker stop {deploy.container_id}")
    if os.path.exists(deploy.datadir):
        shutil.rmtree(deploy.datadir)

    del data[version]
    with open(".deploy_mysqld", "w") as f:
        dump_deploy(data, f)


@dataclass
class Instance:
    url: str
    container_id: str
    cmd: str
    datadir: str


def load_deploy():
    if os.path.exists(".deploy_mysqld"):
        with open(".deploy_mysqld", "r") as f:
            try:
                data = json.load(f)
                for k, v in data.items():
                    data[k] = Instance(**v)

                return data
            except Exception as e:
                print(e)
                return {}
    return {}


def dump_deploy(data, f):
    for k in data:
        data[k] = asdict(data[k])

    json.dump(data, f)


def mDeploy(version):
    deploy_container = load_deploy()
    if version in deploy_container:
        print(
            f"a container of mysqld[{version}] has been deploy at {deploy_container[version]}"
        )
        return

    mContainer = MySqlContainer(f"mysql:{version}")
    datadir = os.getcwd() + f"/datadir/{version}"
    mContainer.with_volume_mapping(datadir, "/var/lib/mysql", "rw")
    os.makedirs(datadir)
    mContainer.with_kwargs(remove=True, user=os.getuid(), userns_mode="host")
    mysql = mContainer.start()
    with open(".deploy_mysqld", "w") as f:
        deploy_container[version] = Instance(
            url=mysql.get_connection_url(),
            container_id=f"{mysql._container.short_id}",
            cmd=f"mysql -h 127.0.0.1 -P{mysql.get_exposed_port(mysql.port)} -u{mysql.username} -p{mysql.password}",
            datadir=datadir,
        )
        dump_deploy(deploy_container, f)


@main.command()
@click.option("--version", type=click.STRING, default="8.0.17")
def deploy(version):
    mDeploy(version)


@main.command()
@click.option("--version", type=click.STRING, default="8.0.17")
@click.option("--sql", type=click.STRING, default="")
def connect(version, sql):
    deploy_container = load_deploy()
    if version not in deploy_container:
        mDeploy(version)
        deploy_container = load_deploy()

    if sql == "":
        os.system(deploy_container.get(version).cmd)
    else:
        os.system(deploy_container.get(version).cmd + f" -e '{sql}'")

@main.command()
@click.option("--version", type=click.STRING, default="")
@click.option("--sql", type=click.STRING, default="")
@click.option("--file", type=click.STRING, default="")
def exec(version, sql, file):
    deploy_container = load_deploy()
    if version not in deploy_container:
        mDeploy(version)
        deploy_container = load_deploy()
    engine = create_engine(deploy_container.get(version).url)
    if file != "":
        with open(file, "r") as f:
            sql = f.read()
    with engine.connect() as conn:
        result = conn.exec_driver_sql(sql)
        print(result.all()[0][1])

@main.command()
@click.option("--version", type=click.STRING, default="")
@click.option("--table", type=click.STRING, default="")
@click.option("--size", type=click.INT, default=100)
@click.option("--idx", type=click.INT, default=-1)
def rand_data(version, table, size, idx):
    deploy_container = load_deploy()
    if version not in deploy_container:
        mDeploy(version)
        deploy_container = load_deploy()

    table_ibd = deploy_container.get(version).datadir + f"/test/{table}.ibd"
    if not os.path.exists(table_ibd):
        print(f"\n\n\n{table} is not exists now, please create first\n\n\n")
        os.system(deploy_container.get(version).cmd)
    else:
        f = open(table_ibd, "rb")
        fsp = disk_struct.MFspPage.parse_stream(f)
        if fsp.sdi_version == 0:
            print("version of mysql is not support")
            return
        f.seek(fsp.sdi_page_no * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        all_tables = [d for d in sdi_page.iterate_sdi_record(f) if d["dd_object_type"] == "Table"]
        if len(all_tables) > 1 and idx == -1:
            print("these is more than one table, please use --idx to specify one")
            return
        elif len(all_tables) == 1:
            idx = 0
        dd_object = Table(**all_tables[idx]["dd_object"])
        sql = dd_object.gen_rand_data_sql(size)
        engine = create_engine(deploy_container.get(version).url)
        with engine.connect() as conn:
            conn.exec_driver_sql(sql)
            conn.commit()
        print(f"insert {size} record randomly into {dd_object.schema_ref}.{dd_object.name}")
                

if __name__ == "__main__":
    main()
