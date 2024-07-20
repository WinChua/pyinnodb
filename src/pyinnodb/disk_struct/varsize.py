from ..mconstruct import *
from .. import const
from .first_page import MFirstPage, MBlobHdr
from .fil import MFil


class OffPagePointer(CC):
    space_id: int = cfield(cs.Int32ub)
    page_num: int = cfield(cs.Int32ub)
    page_offset: int = cfield(cs.Int32ub)
    data_length: int = cfield(cs.Int64ub)

    def read_data(self, stream):
        stream.seek(self.page_num * const.PAGE_SIZE)
        fil = MFil.parse_stream(stream)
        logger.debug("fil is %s", fil)
        if fil.page_type == const.FIL_PAGE_TYPE_BLOB:
            blob_header = MBlobHdr.parse_stream(stream)
            logger.debug("blob header is %s", blob_header)
            return blob_header.get_data(stream)
        else:
            first_page = pointer.get_first_page(stream)
            real_data = first_page.get_data(stream)
            return real_data

    def read(self, stream):
        cur = stream.tell()
        stream.seek(self.page_num * const.PAGE_SIZE + self.page_offset)
        data = stream.read(self.data_length)

        stream.seek(cur)
        return data

    def get_first_page(self, stream) -> MFirstPage:
        stream.seek(self.page_num * const.PAGE_SIZE)
        return MFirstPage.parse_stream(stream)


class VarSize(CC):
    size: int = cfield(cs.Int8ub)

    def _post_parsed(self, stream, context, path):
        if self.size > 0x7F:
            self.size = (self.size - 0x80) * 256 + cs.Int8ub.parse_stream(stream)
