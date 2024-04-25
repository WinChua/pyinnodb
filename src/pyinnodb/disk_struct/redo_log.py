from ..mconstruct import *


class MLogFileHeader(CC):
    group_id: int = cfield(cs.Int32ub)
    start_lsn: int = cfield(cs.Int64ub)
    log_file_number: int = cfield(cs.Int32ub)
    created_by: str = cfield(cstring(32))
    empty: str = cfield(carray(512 - 48 - 4, cs.Int8ub))
    checksum: int = cfield(cs.Int32ub)


class MLogCheckPoint(CC):
    number: int = cfield(cs.Int64ub)
    lsn: int = cfield(cs.Int64ub)
    buffer_size: int = cfield(cs.Int32ub)
    archive_lsn: int = cfield(cs.Int64ub)
    log_group_array: str = cfield(cstring(256))  # unused now
    checksum1: int = cfield(cs.Int32ub)
    checksum2: int = cfield(cs.Int32ub)
    fsp_free_limit: int = cfield(cs.Int32ub)
    fsp_magic_number: int = cfield(cs.Int32ub)
    empty: str = cfield(carray(512 - 300 - 4, cs.Int8ub))
    checksum: int = cfield(cs.Int32ub)


class MLogFile(CC):
    log_header_block: MLogFileHeader = cfield(MLogFileHeader)
    checkpoint_1_block: MLogCheckPoint = cfield(MLogCheckPoint)
    empty_block: str = cfield(carray(512, cs.Int8ub))
    checkpoint_2_block: MLogCheckPoint = cfield(MLogCheckPoint)
