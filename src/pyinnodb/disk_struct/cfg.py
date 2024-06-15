from ..mconstruct import *

class MCfgCol(CC):
    prtype: int = cfield(cs.Int32ub)
    mtype: int = cfield(cs.Int32ub)
    len: int = cfield(cs.Int32ub)
    mbminmaxlen: int = cfield(cs.Int32ub)
    ind: int = cfield(cs.Int32ub)
    ord_part: int = cfield(cs.Int32ub)
    max_prefix: int = cfield(cs.Int32ub)
    name: str = cfield(CLenString(4))
    def _post_parsed(self, stream, context, path):
        version = context.get("version", None)
        if version is None:
            return
        if version >= 7:
            self.version_added = cs.Int8ub.parse_stream(stream)
            self.version_dropped = cs.Int8ub.parse_stream(stream)
            self.phy_pos = cs.Int32ub.parse_stream(stream)
        if version >= 3:
            self.j0 = cs.Int8ub.parse_stream(stream)
            if self.j0 == 0:
                return
            self.j1 = cs.Int8ub.parse_stream(stream)
            if self.j1 == 1:
                self.default = None
            else:
                self.default = CLenString(4).parse_stream(stream)

    def _post_build(self, obj, stream, context, path):
        version = context.get("version", None)
        if version is None:
            return

        if version >= 7:
            stream.write(cs.Int8ub.build(obj.version_added))
            stream.write(cs.Int8ub.build(obj.version_dropped))
            stream.write(cs.Int32ub.build(obj.phy_pos))

        if version >= 3:
            stream.write(cs.Int8ub.build(obj.j0))
            if obj.j0 == 0:
                return
            stream.write(cs.Int8ub.build(obj.j1))
            if obj.j1 == 1:
                return
            stream.write(CLenString(4).build(self.default))


class MCfgIndexField(CC):
    prefix_len: int = cfield(cs.Int32ub)
    fixed_len: int = cfield(cs.Int32ub)
    def _post_parsed(self, stream, context, path):
        version = context.get("version", None)
        if version is None:
            return
        if version >= 4:
            self.is_ascending = cs.Int32ub.parse_stream(stream)
        self.name = CLenString(4).parse_stream(stream)

    def _post_build(self, obj, stream, context, path):
        version = context.get("version", None)
        if version is None:
            return

        if version >= 4:
            stream.write(cs.Int32ub.build(self.is_ascending))

        stream.write(CLenString(4).build(self.name))


class MCfgIndex(CC):
    id: int = cfield(cs.Int64ub)
    space: int = cfield(cs.Int32ub)
    page_no: int = cfield(cs.Int32ub)
    type: int = cfield(cs.Int32ub)
    trx_id_offset: int = cfield(cs.Int32ub)
    n_user_defined_cols: int = cfield(cs.Int32ub)
    n_uniq: int = cfield(cs.Int32ub)
    n_nullable: int = cfield(cs.Int32ub)
    n_fields: int = cfield(cs.Int32ub)
    name: str = cfield(CLenString(4))

    def _post_parsed(self, stream, context, path):
        self.index_fields = []
        for i in range(self.n_fields):
            self.index_fields.append(MCfgIndexField.parse_stream(stream, **context))

    def _post_build(self, obj, stream, context, path):
        for f in self.index_fields:
            stream.write(f.build(**context))


class MCfg(CC):
    version: int = cfield(cs.Int32ub)
    hostname: str = cfield(CLenString(4))
    tablename: str = cfield(CLenString(4))
    autoinc: int = cfield(cs.Int64ub)
    page_size: int = cfield(cs.Int32ub)
    flags: int = cfield(cs.Int32ub)
    n_cols: int = cfield(cs.Int32ub)

    def _post_parsed(self, stream, context, path):
        if self.version >= 5:
            self.n_instant_nullable = cs.Int32ub.parse_stream(stream)

        if self.version >= 7:
            self.initial_column_count = cs.Int32ub.parse_stream(stream)
            self.current_column_count = cs.Int32ub.parse_stream(stream)
            self.total_column_count = cs.Int32ub.parse_stream(stream)
            self.n_instant_drop_cols = cs.Int32ub.parse_stream(stream)
            self.current_row_version = cs.Int32ub.parse_stream(stream)

        if self.version >= 2:
            self.space_flags = cs.Int32ub.parse_stream(stream)
            if self.version >= 6:
                self.compression_type = cs.Int8ub.parse_stream(stream) # 0: NONE, 1: ZLIB, 2: LZ4

        self.cols = []
        for i in range(self.n_cols):
            col = MCfgCol.parse_stream(stream, version = self.version)
            self.cols.append(col)

        self.n_indexes = cs.Int32ub.parse_stream(stream)
        self.indexes = []
        for i in range(self.n_indexes):
            self.indexes.append(MCfgIndex.parse_stream(stream, version=self.version))


    def _post_build(self, obj, stream, context, path):
        if obj.version >= 5:
            stream.write(cs.Int32ub.build(obj.n_instant_nullable))
        if obj.version >= 7:
            stream.write(cs.Int32ub.build(obj.initial_column_count))
            stream.write(cs.Int32ub.build(obj.current_column_count))
            stream.write(cs.Int32ub.build(obj.total_column_count))
            stream.write(cs.Int32ub.build(obj.n_instant_drop_cols))
            stream.write(cs.Int32ub.build(obj.current_row_version))

        if obj.version >= 2:
            stream.write(cs.Int32ub.build(obj.space_flags))
            if obj.version >= 6:
                stream.write(cs.Int8ub.build(obj.compression_type))

        for c in self.cols:
            stream.write(c.build(version=obj.version))

        stream.write(cs.Int32ub.build(self.n_indexes))
        for idx in self.indexes:
            stream.write(idx.build(version=obj.version))

