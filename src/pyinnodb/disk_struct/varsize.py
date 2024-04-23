from ..mconstruct import *
from .. import const

class OffPagePointer(CC):
    space_id : int = cfield(cs.Int32ub)
    page_num : int = cfield(cs.Int32ub)
    page_offset: int = cfield(cs.Int32ub)
    data_length : int = cfield(cs.Int64ub)

    def read(self, stream):
        cur = stream.tell()
        stream.seek(self.page_num * const.PAGE_SIZE + self.page_offset)
        data = stream.read(self.data_length)

        stream.seek(cur)
        return data

class VarSize(CC):
    size : int = cfield(cs.Int8ub)
    
    def _post_parsed(self, stream, context, path):
        if self.size > 0x7F:
            self.size = (self.size - 0x80) * 256 + cs.Int8ub.parse_stream(stream)
