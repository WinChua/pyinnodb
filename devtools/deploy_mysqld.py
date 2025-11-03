import os
import shutil
import click
import json
from pprint import pprint
from pathlib import Path

import docker

from dataclasses import dataclass, asdict
from testcontainers.mysql import MySqlContainer
from testcontainers.core.config import testcontainers_config as c

from sqlalchemy import create_engine, text


from pyinnodb import const
from pyinnodb import disk_struct
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.sdi.table import Table

c.ryuk_disabled = True


def get_project_root():
    return Path(__file__).parent.parent


@click.group()
def main():
    pass


@main.command(name="list")
def tlist():
    data = load_deploy()
    pprint(data)


DATADIR_BASE = get_project_root() / "datadir"


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


@dataclass
class Instance:
    url: str
    container_id: str
    cmd: str
    datadir: str


def load_deploy():
    client = docker.from_env()
    target_versions = {f"mysql:{v}": v for v in os.listdir(DATADIR_BASE)}
    target_instance = {}
    for container in client.containers.list():
        for tag in container.image.tags:
            if tag not in target_versions:
                continue

            port_maps = container.ports.get("3306/tcp", None)
            if port_maps is None:
                continue

            found = False
            for p in port_maps:
                if p.get("HostIp", None) != "0.0.0.0":
                    continue
                port = p.get("HostPort", None)
                if port is None:
                    continue
                target_instance[target_versions[tag]] = Instance(
                    url=f"mysql://test:test@127.0.0.1:{port}/test",
                    container_id=container.short_id,
                    cmd=f"mysql -h 127.0.0.1 -P{port} -utest -ptest test",
                    datadir=target_versions[tag],
                )
                found = True
            if found:
                break

    return target_instance


def mDeploy(version):
    deploy_container = load_deploy()
    if version in deploy_container:
        print(
            f"a container of mysqld[{version}] has been deploy at {deploy_container[version]}"
        )
        return

    mContainer = MySqlContainer(f"mysql:{version}")
    datadir = DATADIR_BASE / f"{version}"
    mContainer.with_volume_mapping(datadir, "/var/lib/mysql", "rw")
    os.makedirs(datadir)
    mContainer.with_kwargs(remove=True, user=os.getuid(), userns_mode="host")
    mysql = mContainer.start()


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
    url = deploy_container.get(version).url
    engine = create_engine(url)
    if file != "":
        if not os.path.isabs(file):
            poe_cwd = os.getenv("POE_CWD")
            if poe_cwd:
                file = os.path.join(poe_cwd, file)
        with open(file, "r") as f:
            sql = f.read()
    with engine.connect() as conn:
        # result = conn.exec_driver_sql(sql)
        result = conn.execute(text(sql))
        conn.commit()
        if result.rowcount == 0:
            print("无结果返回")
        elif result.returns_rows:
            for r in result.fetchall():
                print(r)
        else:
            print("执行成功,影响行数:", result.rowcount)


@main.command()
@click.option("--version", type=click.STRING, default="")
@click.option("--table", type=click.STRING, default="")
@click.option("--size", type=click.INT, default=100)
@click.option("--idx", type=click.INT, default=-1)
@click.option(
    "--random-primary-key/--no-random-primary-key", type=click.BOOL, default=False
)
@click.option("--varsize", type=click.INT, default=None, help="up size of varchar")
def rand_data(version, table, size, idx, random_primary_key, varsize):
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
        all_tables = [
            d for d in sdi_page.iterate_sdi_record(f) if d["dd_object_type"] == "Table"
        ]
        if len(all_tables) > 1 and idx == -1:
            print("these is more than one table, please use --idx to specify one")
            return
        elif len(all_tables) == 1:
            idx = 0
        dd_object = Table(**all_tables[idx]["dd_object"])
        sql = dd_object.gen_rand_data_sql(
            size, rand_primary_key=random_primary_key, varsize=varsize
        )
        engine = create_engine(deploy_container.get(version).url)
        with engine.connect() as conn:
            conn.exec_driver_sql(sql)
            conn.commit()
        print(
            f"insert {size} record randomly into {dd_object.schema_ref}.{dd_object.name}"
        )


if __name__ == "__main__":
    main()
