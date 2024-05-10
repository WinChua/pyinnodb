from context import *

class UL(Base):
    __tablename__ = "ul"
    id = Column(dmysql.types.INTEGER(10), autoincrement=True, primary_key=True)
    name = Column(dmysql.types.VARCHAR(255))


def test_undo(containerOp: ContainerOp):
    Base.metadata.create_all(containerOp.engine, [UL.__table__])
    containerOp.build_ibd(
        insert(UL).values(
            name = "WinChua",
        )
    )
    # containerOp.build_ibd(
    #         update(UL).where(UL.name == "WinChua").values(name = "OK"),
    # )
    tar = containerOp.open(containerOp.build_data_path("test/ul.ibd"))
    undo1 = containerOp.open(containerOp.build_data_path("undo_001"))

    with open("ul.ibd", "wb") as f:
        f.write(tar.read())

    with open("undo_001", "wb") as f:
        f.write(undo1.read())
