from .fil import MFil
from ..mconstruct import *
import typing as t


class MIBufEntry2(CC):
    free_space_0: int = cfield(cs.BitsInteger(2))
    buffer_flag_0: int = cfield(cs.BitsInteger(1))
    change_buffer_flag_0: int = cfield(cs.BitsInteger(1))
    free_space_1: int = cfield(cs.BitsInteger(2))
    buffer_flag_1: int = cfield(cs.BitsInteger(1))
    change_buffer_flag_1: int = cfield(cs.BitsInteger(1))


class MIBufPage(CC):
    fil: MFil = cfield(MFil)
    change_buffer_bitmap: t.List[MIBufEntry2] = cfield(
        carray(4096, MIBufEntry2))
