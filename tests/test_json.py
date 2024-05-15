from context import *


class JT(Base):
    __tablename__ = "jt"

    id = Column(dmysql.types.INTEGER(10), autoincrement=True, primary_key=True)
    JSON_F = Column(dmysql.JSON)
    V_F = Column(dmysql.INTEGER(10))


def test_json_t(containerOp: ContainerOp):
    Base.metadata.create_all(containerOp.engine, [JT.__table__])
    containerOp.build_ibd(
        insert(JT).values(
            JSON_F={"name": "WinChua", "hello": [{"你好": 42}]},
            V_F=42,
        ),
        insert(JT).values(
            V_F = 43,
            JSON_F = [1,2,3,4],
        ),
        insert(JT).values(
            V_F = 44,
            JSON_F = "HELLO",
        ),
        insert(JT).values(
            V_F = 44,
            JSON_F = 42,
        ),
        insert(JT).values(
            V_F = 90,
            JSON_F = 3.14,
        ),
        insert(JT).values(
            JSON_F = [{"K"*8096: [3.14, {"HELLO":[{"array":["Hello", 3.15, {"A": "B"}]}, "data", 53]}]}, 90],
            V_F = 89,
        ),
    )
    time.sleep(3)
    tar = containerOp.open(containerOp.build_data_path("/test/jt.ibd"))
    with open("jt.ibd", "wb") as f:
        f.write(tar.read())
