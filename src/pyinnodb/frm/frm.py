from ..mconstruct import *
from io import BytesIO
from pyinnodb.sdi.table import Column, Index, IndexElement, ColumnElement
import typing
from pyinnodb.const.index_type import IndexType
from pyinnodb import const
import enum
from base64 import b64encode

# header_size: 64
# forminfo_offset: header_size+${names_length}
# forminfo_size: 288


class MFrmKeyParts(CC):
    nr: int = cfield(cs.Int16ul) #  -1: column idx of primary
    offset: int = cfield(cs.Int16ul)
    flags: int = cfield(cs.Int8ul)
    key_type: int = cfield(cs.Int16ul)
    length: int = cfield(cs.Int16ul) # primary key length

    def _post_parsed(self, stream, context, path):
        self.nr &= 0x3fff
        self.offset -= 1

class FieldFlag(enum.Enum):
    DECIMAL = 1
    BINARY = 1
    NUMBER = 2
    ZEROFILL = 4
    PACK = 120
    INTERVAL = 256
    BITFIELD = 512
    BLOB = 1024
    GEOM = 2048
    TREAT_BIT_AS_CHAR = 4096
    # defined, but not used in modern MySQL
    # LEFT_FULLSCREEN = 8192
    NO_DEFAULT = 16384
    # defined, but not used in modern MySQL
    # FORMAT_NUMBER = 16384
    # defined, but not used in modern MySQL
    # RIGHT_FULLSCREEN = 16385
    # defined, but not used in modern MySQL
    # SUM = 32768
    MAYBE_NULL = 32768
    HEX_ESCAPE = 0X10000
    PACK_SHIFT = 3
    DEC_SHIFT = 8
    MAX_DEC = 31
    NUM_SCREEN_TYPE = 0X7F01
    ALFA_SCREEN_TYPE = 0X7800


class MFrmColumn(CC):  # 17
    padding1: str = cfield(cs.Bytes(3))
    length: int = cfield(cs.Int16ul)
    default_offset: int = cfield(IntFromBytes(3, "little"))
    flags: int = cfield(cs.Int16ul)
    unireg_check: int = cfield(cs.Int8ul)
    charset_id1: int = cfield(cs.Int8ul)
    label_id: int = cfield(cs.Int8ul)
    type_code: int = cfield(cs.Int8ul)
    charset_id2: int = cfield(cs.Int8ul)
    comment_length: str = cfield(cs.Bytes(2))

    def to_dd_column(self, name: str, pos: int, labels: typing.List[typing.List[str]]) -> Column:
        c = Column()
        c.hidden = const.column_hidden_type.ColumnHiddenType.HT_VISIBLE.value
        c.ordinal_position = pos
        if self.type_code < 20:
            c.type = self.type_code + 1
        else:
            c.type = self.type_code - 225
        c.name = name
        if c.type == const.dd_column_type.DDColumnType.NEWDECIMAL.value:
            c.numeric_precision = self.length
            c.numeric_scale = (int(self.flags) >> 8) & 31
            if c.numeric_scale:
                c.numeric_precision -= 1
            if c.numeric_precision:
                c.numeric_precision -= 1
        elif c.type == [const.dd_column_type.DDColumnType.ENUM.value, 
                const.dd_column_type.DDColumnType.SET.value]:
            if self.label_id <= len(labels):
                for i, name in enumerate(labels[self.label_id-1]):
                    c.elements.append(ColumnElement(name=b64encode(name), index=i+1))


        c.is_nullable = bool(self.flags & FieldFlag.MAYBE_NULL.value)

        return c

class MFrmKey(CC):
    flags: int = cfield(cs.Int16ul) # & 1 => uniq
    length: int = cfield(cs.Int16ul)
    parts_count: int = cfield(cs.Int8ul)
    algorithm: int = cfield(cs.Int8ul)
    key_block_size: int = cfield(cs.Int16ul)

    def get_index_type(self):
        if self.flags & 128:
            return IndexType.IT_FULLTEXT.value
        elif self.flags & 1024:
            return IndexType.IT_SPATIAL.value
        return IndexType.IT_PRIMARY.value

    def _post_parsed(self, stream, context, path):
        self.flags ^= 1
        self.key_parts = []
        for i in range(self.parts_count):
            self.key_parts.append(MFrmKeyParts.parse_stream(stream))

    def to_dd_index(self, name:str, cols: typing.List[MFrmColumn]) -> Index:
        idx = Index()
        idx.name = name
        idx.type = self.get_index_type()
        kp = {}
        for k in self.key_parts:
            kp[k.nr - 1] = k
            ie = IndexElement()
            ie.length = k.length
            ie.column_opx = k.nr - 1
            ie.ordinal_position = len(idx.elements) + 1
            idx.elements.append(ie)

        if len(idx.elements) == 0:
            pass # TODO: ROW_ID

        idx.elements.append(IndexElement(
            length = 4294967295,
            column_opx = len(cols),
            hidden = True,
            ordinal_position = len(cols) + 1,
        ))

        idx.elements.append(IndexElement(
            length = 4294967295,
            column_opx = len(cols) + 1,
            hidden = True,
            ordinal_position = len(cols) + 2,
        ))

        for i, c in enumerate(cols):
            if i in kp:
                continue
            ie = IndexElement()
            ie.column_opx = i
            ie.ordinal_position = len(idx.elements) + 1
            ie.hidden = False
            ie.length = 4294967295
            idx.elements.append(ie)

        return idx


class MFrm(CC):
    magic: str = cfield(cs.Bytes(2))
    padding1: str = cfield(cs.Bytes(1))
    legacy_db_type: int = cfield(cs.Int8ul)  # 0x03, 1
    names_length: int = cfield(cs.Int16ul)  # 0x04, 2
    keyinfo_offset: int = cfield(cs.Int16ul)  # 0x06, 2
    padding2: str = cfield(cs.Bytes(6))
    keyinfo_length: int = cfield(cs.Int16ul)  # 0x0e, 2
    defaults_length: int = cfield(cs.Int16ul)  # 0x10, 2
    max_rows: int = cfield(cs.Int32ul)  # 0x12, 4
    min_rows: int = cfield(cs.Int32ul)  # 0x16, 4
    padding3: str = cfield(cs.Bytes(4))  # 0x1a, 4
    handler_opt: int = cfield(cs.Int16ul)  # 0x1e, 2
    padding3_: str = cfield(cs.Bytes(2))  # 0x20, 2
    avg_row_length: int = cfield(cs.Int32ul)  # 0x22, 4
    charset_id: int = cfield(cs.Int8ul)  # 0x26, 1
    padding_: str = cfield(cs.Bytes(1))  # 0x27, 1
    row_format: int = cfield(cs.Int8ul)  # 0x28, 1
    padding4: str = cfield(cs.Bytes(10))  # 0x29, 12
    version: int = cfield(cs.Int32ul)  # 0x033, 4
    extrainfo_length: int = cfield(cs.Int32ul)  # 0x37, 4
    padding5: str = cfield(cs.Bytes(3))  # 0x3b, 5
    key_block_size: int = cfield(cs.Int16ul)

    def _post_parsed(self, stream, context, path):
        cur = stream.tell()
        self.table_name = stream.read(self.names_length)
        forminfo_offset = cs.Int32ul.parse_stream(stream)
        # table comment
        stream.seek(forminfo_offset + 46)
        table_comment_length = cs.Int8ub.parse_stream(stream)
        if table_comment_length != 0xff:
            table_comment = stream.read(table_comment_length)



        # other
        stream.seek(forminfo_offset + 258)
        column_count = cs.Int16ul.parse_stream(stream)
        screens_length = cs.Int16ul.parse_stream(stream)
        stream.seek(6, 1)
        names_length = cs.Int16ul.parse_stream(stream)
        stream.seek(4, 1)
        labels_length = cs.Int16ul.parse_stream(stream)
        stream.seek(6, 1)
        null_fields = cs.Int16ul.parse_stream(stream)
        comment_length = cs.Int16ul.parse_stream(stream)
        stream.seek(2 + screens_length, 1)
        metadata_length = 17 * column_count
        cols = []
        for i in range(column_count):
            cols.append(MFrmColumn.parse_stream(stream))
        self.cols = cols
        names = stream.read(names_length)
        labels = stream.read(labels_length)
        comment = stream.read(comment_length)


        if self.keyinfo_length == 0xFFFF:
            stream.seek(0x02F)
            self.keyinfo_length = cs.Int32ul.parse_stream(stream)

        stream.seek(self.keyinfo_offset)
        # key
        self.keyinfo = stream.read(self.keyinfo_length)

        key_stream = BytesIO(self.keyinfo)
        key_count = cs.Int8ul.parse_stream(key_stream)
        if key_count < 128:
            key_parts_count = cs.Int8ul.parse_stream(key_stream)
            key_stream.read(2)
        else:
            key_count = (key_count & 0x7F) | (cs.Int8ul.parse_stream(key_stream) << 7)
            key_parts_count = cs.Int16ul.parse_stream(key_stream)

        key_extra_length = cs.Int16ul.parse_stream(key_stream)

        keys = []
        for i in range(key_count):
            keys.append(MFrmKey.parse_stream(key_stream))



        key_extra = key_stream.read(key_extra_length)

        key_name, key_comment = key_extra.split(b"\x00", 1)

        key_names = [name for name in key_name.split(b"\xff") if name]

        comment_stream = BytesIO(key_comment)
        key_comments = []
        for key in keys:
            if key.flags ^ 4096:
                key_comments.append(CLenString(2, "little").parse_stream(comment_stream))
            else:
                key_comments.append("")

        self.keys = list(zip(keys, key_names, key_comments))

        # default
        self.defaults = stream.read(self.defaults_length)
        extrainfo_offset = stream.tell()
        self.extrainfo = stream.read(self.extrainfo_length)

        self.connection = None
        self.engine = None
        self.partition_info = None
        if self.extrainfo_length > 0:
            self.connection = CLenString(2, "little").parse_stream(stream)
            if stream.tell() - extrainfo_offset < self.extrainfo_length:
                self.engine = CLenString(2, "little").parse_stream(stream)
                if stream.tell() - extrainfo_offset < self.extrainfo_length:
                    self.partition_info = CLenString(4, "little").parse_stream(stream)
            stream.read(2)

        column_names = names[1:-2].split(b"\xff")
        self.column_labels = [
            g.split(b"\xff") for g in labels[1:-2].split(b"\x00") if g
        ]


        _null_bytes = (null_fields + 1) // 8

        for i, c_name in enumerate(column_names):
            cols[i].name = c_name.decode()



# <BBBBHHIHHIHHHHHBBIBBBBBIIIIBBBHH
class MFrmHeader(CC):
    frm_version: int = cfield(cs.Int8ul)
    engine_name: int = cfield(cs.Int8ul)
    padding2: int = cfield(cs.Int8ul)
    padding3: int = cfield(cs.Int8ul)
    io_size: int = cfield(cs.Int16ul)
    padding5: int = cfield(cs.Int16ul)
    length: int = cfield(cs.Int32ul)
    tmp_key_length: int = cfield(cs.Int16ul)
    rec_length: int = cfield(cs.Int16ul)
    padding9: int = cfield(cs.Int32ul)
    max_rows: int = cfield(cs.Int16ul)
    min_rows: int = cfield(cs.Int16ul)
    db_create_pack: int = cfield(cs.Int16ul)
    key_info_length: int = cfield(cs.Int16ul)
    create_options: int = cfield(cs.Int16ul)
    padding15: int = cfield(cs.Int8ul)
    frm_file_ver: int = cfield(cs.Int8ul)
    avg_row_length: int = cfield(cs.Int32ul)
    default_charset: int = cfield(cs.Int8ul)
    padding19: int = cfield(cs.Int8ul)
    row_type: int = cfield(cs.Int8ul)
    charset_low: int = cfield(cs.Int8ul)
    padding22: int = cfield(cs.Int8ul)
    padding23: int = cfield(cs.Int32ul)
    key_length: int = cfield(cs.Int32ul)
    mysql_version_id: int = cfield(cs.Int32ul)
    extra_size: int = cfield(cs.Int32ul)
    padding27: int = cfield(cs.Int8ul)
    padding28: int = cfield(cs.Int8ul)
    default_part_eng: int = cfield(cs.Int8ul)
    key_block_size: int = cfield(cs.Int16ul)
    padding31: int = cfield(cs.Int16ul)
