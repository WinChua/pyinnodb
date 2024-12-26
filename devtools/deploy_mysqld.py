import os
import sys
import shutil
import click
import json
from pprint import pprint

from dataclasses import dataclass, asdict
from testcontainers.mysql import MySqlContainer
from docker.models.containers import Container

@click.group()
def main():
    pass

@main.command()
def list():
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
        print(f"a container of mysqld[{version}] has been deploy at {deploy_container[version]}")
        return


    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
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
def connect(version):
    deploy_container = load_deploy()
    if version not in deploy_container:
        mDeploy(version)
        deploy_container = load_deploy()

    os.system(deploy_container.get(version).cmd)

if __name__ == "__main__":
    main()


