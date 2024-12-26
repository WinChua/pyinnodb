from ..mconstruct import *
from .fil import MFil
from .list import MPointer
import typing


class MRSegEntry(CC):
    space: int = cfield(cs.Int32ub)
    page: int = cfield(cs.Int32ub)


class MTrxSysPage(CC):
    fil: MFil = cfield(MFil)
    transaction_id: int = cfield(cs.Int64ub)
    trx_sys_spaceid: int = cfield(cs.Int32ub)
    trx_sys_pointer: MPointer = cfield(MPointer)
    RSeg_array: typing.List[MRSegEntry] = cfield(carray(128, MRSegEntry))
