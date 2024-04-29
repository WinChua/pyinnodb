from context import *
import random

al = "abcdefghijklmnopqrstuvwxyz"


class SD(Base):
    __tablename__ = "sd"
    id = Column(dmysql.types.INTEGER(10), autoincrement=True, primary_key=True)
    name = Column(dmysql.types.VARCHAR(255))


def test_delete(containerOp: ContainerOp):
    Base.metadata.create_all(containerOp.engine, [SD.__table__])
    for i in range(100):
        sql = []
        for i in range(200):
            sql.append(
                insert(SD).values(
                    name="".join(random.sample(al, 10)),
                )
            )
        containerOp.build_ibd(*sql, nosleep=True)

    time.sleep(3)
    tar = containerOp.open(containerOp.build_data_path("/test/sd.ibd"))
    with open("sd.ibd", "wb") as f:
        f.write(tar.read())
