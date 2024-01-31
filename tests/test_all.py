from context import *
from pyinnodb.struct.fil import MFil
from pyinnodb.struct.fsp import MFspHeader
from pyinnodb.struct.ibuf import MIBufEntry2, MIBufPage
from pyinnodb.struct.index import MIndexHeader, MIndexPage, MFsegHeader, MSystemRecord, MIndexSystemRecord
from pyinnodb.struct.inode import MInodeEntry
from pyinnodb.struct.list import MListBaseNode, MListNode, MPointer
from pyinnodb.struct.xdes import MXdesEntry
from pyinnodb.struct.record import MRecordHeader


def test_all_size():
    assert MFil.sizeof() == 38
    assert MFspHeader.sizeof() == 112
    assert MIBufEntry2.sizeof() == 1
    assert MIndexHeader.sizeof() == 36
    assert MFsegHeader.sizeof() == 20
    assert MSystemRecord.sizeof() == 13
    assert MIndexSystemRecord.sizeof() == 26
    assert MInodeEntry.sizeof() == 192
    assert MListBaseNode.sizeof() == 16
    assert MListNode.sizeof() == 12
    assert MPointer.sizeof() == 6
    assert MXdesEntry.sizeof() == 40
    assert MRecordHeader.sizeof() == 5
