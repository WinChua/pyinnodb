from .. import const
from ..mconstruct import *
from .undo_log import MUndoRecordInsert, TRX_UNDO_DEL_MARK_REC

UNIV_SQL_NULL = ~0
UNIV_PAGE_SIZE_SHIFT_DEF = 14
UNIV_PAGE_SIZE_DEF = 1 << UNIV_PAGE_SIZE_SHIFT_DEF
UNIV_EXTERN_STORAGE_FIELD = int.from_bytes(
    (UNIV_SQL_NULL - UNIV_PAGE_SIZE_DEF).to_bytes(4, "big", signed=True), "big"
)
SPATIAL_STATUS_SHIFT = 12
SPATIAL_STATUS_MASK = 3 << SPATIAL_STATUS_SHIFT


class HistoryVersion:
    def __init__(self, trx_id, rollptr, upd):
        self.trx_id = trx_id
        self.field = []
        self.rollptr = rollptr
        self.upd = upd

    def add_field(self, col, value):
        self.field.append((col, value))

    def __str__(self):
        if self.upd == 1:
            return f"<Update by trx[{self.trx_id}]: {'; '.join('`' + c.name + '` updated original value: ' + str(v) for c, v in self.field)}>"
        else:
            return "<Insert>"

    __repr__ = __str__


class MRollbackPointer(CC):
    insert_flag: int = cfield(cs.BitsInteger(1))
    rollback_seg_id: int = cfield(cs.BitsInteger(7))
    page_number: int = cfield(cs.Int32ub)
    page_offset: int = cfield(cs.Int16ub)

    def last_version(self, undo_map, primary_col, disk_data_layout):
        f = undo_map[self.rollback_seg_id]
        f.seek(self.page_number * const.PAGE_SIZE + self.page_offset)
        undo_record_header = MUndoRecordInsert.parse_stream(f)
        if self.insert_flag == 0:
            # trx_undo_prev_version_build
            # trx_undo_update_rec_get_sys_cols
            # trx_undo_rec_get_col_val
            info_bits = f.read(1)
            trx_id = const.mach_u64_read_next_compressed(f)
            ptr = const.mach_u64_read_next_compressed(f)
            ptr = MRollbackPointer.parse(ptr.to_bytes(7, "big"))
            hist = HistoryVersion(trx_id, ptr, 1)
            # trx_undo_rec_skip_row_ref: skip primary key
            for c in primary_col:
                col_len = const.read_compressed_mysql_int(f)
                col_data_skip = f.read(col_len)
                logger.debug(
                    f"skip primary key: {c.name}, len: {col_len}, data: {col_data_skip}"
                )

            # trx_undo_update_rec_get_update
            type = undo_record_header.flag & 0xF
            if type != TRX_UNDO_DEL_MARK_REC:
                n_fields = const.read_compressed_mysql_int(f)
            else:
                n_fields = 0
            logger.debug(f"undo record type: {type}, update field cnt: {n_fields}")
            for i in range(n_fields):
                col_no = const.read_compressed_mysql_int(f)
                len = const.read_compressed_mysql_int(f)
                # if len > UNIV_EXTERN_STORAGE_FIELD:
                #    len = ((len - UNIV_EXTERN_STORAGE_FIELD) & (~SPATIAL_STATUS_MASK))
                col = disk_data_layout[col_no][0]
                if len == const.FFFFFFFF:  # UNIV_SQL_NULL & 0xffffffff
                    orig_data = None
                else:
                    orig_data = col.read_data(f, len)
                hist.add_field(col, orig_data)
                # trx_undo_rec_get_col_val
            return hist, ptr
        else:
            return HistoryVersion(None, None, 0), None
