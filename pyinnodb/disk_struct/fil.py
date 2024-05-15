from ..mconstruct import *


class MFil(CC):
    checksum: int = cfield(cs.Int32ub)
    offset: int = cfield(cs.Int32ub)
    pre_page: int = cfield(cs.Int32ub)
    next_page: int = cfield(cs.Int32ub)
    lsn: int = cfield(cs.Int64ub)
    page_type: int = cfield(cs.Int16ub)
    flush_lsn: int = cfield(cs.Int64ub)
    spaceid: int = cfield(cs.Int32ub)


class MFilTrailer(CC):
    old_checksum: int = cfield(cs.Int32ub)
    low_32_bits_lsn: int = cfield(cs.Int32ub)
