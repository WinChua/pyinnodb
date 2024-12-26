import dataclasses
import construct as cs
import typing as t
from construct_typed import csfield

METAKEY = "subcon"
SHOW_FIELDS = "_show_fields"


class CMeta(type):
    def __new__(cls, name, bases, attrs):
        klass = super().__new__(cls, name, bases, attrs)
        klass = dataclasses.dataclass(klass)
        fields = dataclasses.fields(klass)
        sub_fields = {}
        klass._bits_names = []
        tmp_fields = []
        for f in fields:
            subcon = f.metadata.get(METAKEY, None)
            if subcon is None:
                continue
            if isinstance(subcon, cs.BitsInteger):
                if len(tmp_fields) == 0 or not isinstance(tmp_fields[-1], list):
                    tmp_fields.append([])
                tmp_fields[-1].append(f)
            else:
                tmp_fields.append(f)

        for f in tmp_fields:
            if isinstance(f, list):
                name = "_".join(map(lambda x: x.name, f))
                klass._bits_names.append(name)
                subcon = cs.BitStruct(**{k.name: k.metadata[METAKEY] for k in f})
            else:
                name = f.name
                subcon = f.metadata[METAKEY]
            sub_fields[name] = subcon

        klass.subcon = CAdaptor(klass, cs.Struct(**sub_fields))

        repr = klass.__repr__

        def get_subcon():
            return klass.subcon

        klass._get_subcon = get_subcon

        def __repr__(self):
            orepr = repr(self)
            other = []
            for k in getattr(self, SHOW_FIELDS, []):
                v = getattr(self, k, None)
                if v:
                    other.append(f"{k}={v}")
            if len(other) > 0:
                orepr = orepr[:-1] + ", " + ", ".join(other) + ")"
            return orepr

        klass.__repr__ = __repr__

        def _set_show_field(self, k, v):
            if getattr(self, SHOW_FIELDS, None) is None:
                setattr(self, SHOW_FIELDS, [])

            setattr(self, k, v)
            if k not in self._show_fields:
                self._show_fields.append(k)

        klass._set_show_field = _set_show_field

        return klass


pt = t.TypeVar("pt")


class CAdaptor(cs.Adapter):
    def __init__(self, cls, subcon):
        super().__init__(subcon)
        self._cls = cls
        if getattr(cls, "parsed", None) is not None:
            self.parsed = cls.parsed

    def _parse(self, stream, context, path):
        obj = super()._parse(stream, context, path)
        if getattr(obj, "_post_parsed", None) is not None:
            v = obj._post_parsed(stream, context, path)
            if v is not None:
                setattr(obj, "_post_value", v)
        return obj

    def _build(self, obj, stream, context, path):
        obj = super()._build(obj, stream, context, path)
        if getattr(obj, "_post_build", None) is not None:
            obj._post_build(obj, stream, context, path)

        return obj

    def _decode(self, obj, context, path):
        fields = dataclasses.fields(self._cls)
        init_f = {}
        not_init_f = {}
        for f in fields:
            bn = self._cls._get_bits_name_map(f.name)
            if bn is None:  # not bit
                v = obj.get(f.name)
            else:
                v = obj.get(bn, {}).get(f.name)
            if f.init:
                init_f[f.name] = v
            else:
                not_init_f[f.name] = v

        dc = self._cls(**init_f)
        for k, v in not_init_f.items():
            setattr(dc, k, v)
        return dc

    def _encode(self, obj, context, path):
        if obj is None:
            obj = self
        return {f.name: getattr(obj, f.name) for f in dataclasses.fields(self._cls)}


class CC(cs.Construct, metaclass=CMeta):
    parsed = None  # don't overload this, as construct will call it twice under this situation
    # "fieldname" / Field
    flagbuildnone = None

    @classmethod
    def sizeof(cls, **contextkw):
        return cls._get_subcon().sizeof(**contextkw)

    @classmethod
    def _get_bits_name_map(cls, name):
        for n in cls._bits_names:
            if name in n:
                return n
        return None

    @classmethod
    def parse(cls: pt, data, **contextkw) -> pt:
        return super().parse(cls, data, **contextkw)

    @classmethod
    def parse_stream(cls: pt, stream, **contextkw) -> pt:
        return super().parse_stream(cls, stream, **contextkw)

    @classmethod
    def _parsereport(cls: pt, stream, context, path) -> pt:
        return super()._parsereport(cls, stream, context, path)

    @classmethod
    def _parse(cls: pt, stream, context, path) -> pt:
        obj = cls._get_subcon()._parse(stream, context, path)
        return obj

    def _post_parsed(self, stream, context, path):
        pass

    def _encode(self, obj=None, context=None, path=None):
        if obj is None:
            obj = self
        return {
            f.name: getattr(obj, f.name) for f in dataclasses.fields(self.__class__)
        }

    @classmethod
    def _decode(cls, obj, context, path):
        fields = dataclasses.fields(cls)
        init_f = {}
        not_init_f = {}
        for f in fields:
            bn = cls._get_bits_name_map(f.name)
            if bn is None:  # not bit
                v = obj.get(f.name)
            else:
                v = obj.get(bn, {}).get(f.name)
            if f.init:
                init_f[f.name] = v
            else:
                not_init_f[f.name] = v

        dc = cls(**init_f)
        for k, v in not_init_f.items():
            setattr(dc, k, v)
        return dc

    def build(self, obj=None, **contextkw):
        if obj is None:
            obj = self
        return super().build(obj, **contextkw)

    def _build(self, obj, stream, context, path):
        # obj = self._encode(obj, context, path)
        # print(obj)
        self.subcon._build(obj, stream, context, path)
        # if getattr(self, "_post_build", None) is not None:
        #     self._post_build(obj, stream, context, path)


def cstring(size):
    return cs.PaddedString(size, "utf8")


def carray(count, subcon):
    if isinstance(subcon, CMeta):
        subcon = subcon._get_subcon()
    return cs.Array(count, subcon)


def cfield(subcon, default=None):
    if isinstance(subcon, cs.Array):
        if isinstance(subcon.subcon, CMeta):
            subcon = cs.Array(subcon.count, subcon.subcon._get_subcon())
    elif isinstance(subcon, CMeta):
        return dataclasses.field(
            # default=default,
            metadata={
                METAKEY: subcon._get_subcon(),
            }
        )
    return csfield(subcon)


class CLenString(cs.Construct):
    def __init__(self, len_size, byte_order="big"):
        super().__init__()
        self._len_size = len_size
        self._byte_order = byte_order

    def _parse(self, stream, context, path):
        len_bytes = stream.read(self._len_size)
        data_size = int.from_bytes(len_bytes, self._byte_order)
        data = stream.read(data_size)
        return data

    def _build(self, obj, stream, context, path):
        data_size = len(obj)
        stream.write(int.to_bytes(data_size, self._len_size, self._byte_order))
        stream.write(obj)


class IntFromBytes(cs.Construct):
    def __init__(self, length, byte_order="big"):
        super().__init__()
        self.length = length
        self._byte_order = byte_order

    def _parse(self, stream, context, path):
        return int.from_bytes(stream.read(self.length), self._byte_order)

    def _build(self, obj, stream, context, path):
        v = int.to_bytes(obj, self.length, self._byte_order)
        stream.write(v)

    def _sizeof(self, context, path):
        return self.length


if __name__ == "__main__":

    @dataclasses.dataclass
    class D:
        age: int
        name: str

    D.__str__

    class DD(CC):
        f: int = cfield(cs.Int16ub)

    class DDD(CC):
        dd: DD = cfield(DD)

        def _post_parsed(self, stream, context, path):
            print("hello", self.dd)

    print("-------------start--------------")
    testdata = b"\x00\x01\x02\x03"
    dd = DDD.parse(b"\x00\x01\x01\x01")
    print(dd.build())
    print(DDD(dd=DD(f=1)).build())

    class Array(CC):
        fs: t.List[DD] = cfield(carray(10, DD))
        cstr: str = cfield(cstring(10))

    print(Array.sizeof())
    ta = Array(fs=[DD(f=1)] * 10, cstr="a" * 10)
    print(ta.build())
