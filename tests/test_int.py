from context import *
import decimal

class IntValue(Base):
    __tablename__ = "int_value"

    SIGN_INT = Column(dmysql.types.INTEGER(11), primary_key=True, autoincrement="auto")
    UNSIGN_INT = Column(dmysql.types.INTEGER(11, unsigned=True))
    SIGN_TINY_INT = Column(dmysql.types.TINYINT(11))
    UNSIGN_TINY_INT = Column(dmysql.TINYINT(11, unsigned=True))
    BIG_INT = Column(dmysql.types.BIGINT(11))
    UNSIGN_BIG_INT = Column(dmysql.types.BIGINT(11, unsigned=True))
    MEDIUM_INT = Column(dmysql.types.MEDIUMINT(11))
    UNSIGN_MEDIUM_INT = Column(dmysql.types.MEDIUMINT(11, unsigned=True))
    SMALL_INT = Column(dmysql.types.SMALLINT(11))
    UNSIGN_SMALL_INT = Column(dmysql.types.SMALLINT(11, unsigned=True))
    VARCHAR_F = Column(dmysql.types.VARCHAR(255))
    VARCHAR_D = Column(dmysql.types.VARCHAR(10))

    FLOAT_F = Column(dmysql.types.FLOAT(39))
    DOUBLE_F = Column(dmysql.types.DOUBLE(19, 13))

    DECIMAL_F = Column(dmysql.types.DECIMAL(30, 15))


def test_int_value(containerOp: ContainerOp):
    logger.info("create int_value table is %s", CreateTable(IntValue.__table__).compile(dialect=dmysql.dialect()))
    Base.metadata.create_all(containerOp.engine, [IntValue.__table__])
    containerOp.build_ibd(
        insert(IntValue).values(FLOAT_F = 3.1415926, DECIMAL_F = -1234567890.12345678910, # python will truncate to -1234567890.1234567
            UNSIGN_TINY_INT=3, BIG_INT=-9881263181, UNSIGN_BIG_INT=89223823943, VARCHAR_F = "我们", VARCHAR_D = "OKS",
            MEDIUM_INT=90, UNSIGN_MEDIUM_INT=12, SMALL_INT=-23, UNSIGN_SMALL_INT=17),
        insert(IntValue).values(FLOAT_F = 3.1415926, DECIMAL_F = -1234567890.12345678910, # python will truncate to -1234567890.1234567
            SIGN_TINY_INT=44, UNSIGN_TINY_INT=3, BIG_INT=-9881263181, UNSIGN_BIG_INT=89223823943, VARCHAR_F = "helloWorld",
            MEDIUM_INT=90, UNSIGN_MEDIUM_INT=12, SMALL_INT=-23, UNSIGN_SMALL_INT=17),
        ## insert(IntValue).values(UNSIGN_INT=43, DOUBLE_F = 2.32112, FLOAT_F = 3.1415926, DECIMAL_F = decimal.Decimal('1234567890.12345678910'),
        ##     SIGN_TINY_INT=44, UNSIGN_TINY_INT=3, BIG_INT=-9881263181, UNSIGN_BIG_INT=89223823943,
        ##     MEDIUM_INT=90, UNSIGN_MEDIUM_INT=12, SMALL_INT=-23, UNSIGN_SMALL_INT=17),
        ## insert(IntValue).values(UNSIGN_INT=43, DOUBLE_F = 2.32112, FLOAT_F = 3.1415926, DECIMAL_F = 1234567890.1234,
        ##     SIGN_TINY_INT=44, UNSIGN_TINY_INT=3, BIG_INT=-9881263181, UNSIGN_BIG_INT=89223823943,
        ##     MEDIUM_INT=90, UNSIGN_MEDIUM_INT=12, SMALL_INT=-23, UNSIGN_SMALL_INT=17),
        ## insert(IntValue).values(UNSIGN_INT=43, DOUBLE_F = 2.32112, FLOAT_F = 3.1415926, DECIMAL_F = 10.2,
        ##     SIGN_TINY_INT=44, UNSIGN_TINY_INT=3, BIG_INT=-9881263181, UNSIGN_BIG_INT=89223823943,
        ##     MEDIUM_INT=90, UNSIGN_MEDIUM_INT=12, SMALL_INT=-23, UNSIGN_SMALL_INT=17),
        ## insert(IntValue).values(UNSIGN_INT=43, DOUBLE_F = 2.32112, FLOAT_F = 3.1415926, DECIMAL_F = -10.2,
        ##     SIGN_TINY_INT=44, UNSIGN_TINY_INT=3, BIG_INT=-9881263181, UNSIGN_BIG_INT=89223823943,
        ##     MEDIUM_INT=90, UNSIGN_MEDIUM_INT=12, SMALL_INT=-23, UNSIGN_SMALL_INT=17),
    )
    time.sleep(3)
    tar = containerOp.open(containerOp.build_data_path("/test/int_value.ibd"))
    with open("int_value.ibd", "wb") as f:
        f.write(tar.read())


class SV(Base):
    __tablename__ = "SVOVER"
    id = Column(dmysql.types.INTEGER(10), primary_key=True)
    VAR_F = Column(dmysql.types.LONGBLOB(16300))
    INT_F = Column(dmysql.types.INTEGER(10))

def test_varchar_overflow(containerOp: ContainerOp):
    Base.metadata.create_all(containerOp.engine, [SV.__table__])
    containerOp.build_ibd(
        insert(SV).values(
            VAR_F = b"z" * 8090, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8091, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8092, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8093, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8094, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8095, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8096, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8097, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8098, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8099, INT_F = 42,
        ),
        insert(SV).values(
            VAR_F = b"z" * 8115, INT_F = 42,
        ),
    )
    time.sleep(3)
    tar = containerOp.open(containerOp.build_data_path("/test/SVOVER.ibd"))
    with open("SVOVER.ibd", "wb") as f:
        f.write(tar.read())
