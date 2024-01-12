from bisect import bisect
from elftools import construct
from elftools.construct import Construct
from elftools.common.utils import struct_parse

import logging

logger = logging.getLogger(__name__)


class MBits:
    def __init__(self, name, length):
        self.name = name
        self.length = length

    def _make_con(self):
        return construct.Bits(self.name, self.length)


def Bits(length):
    return lambda name: MBits(name, length)


class Field(object):
    creation_counter = 0

    def __init__(self, parser, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.create_counter = Field.creation_counter
        Field.creation_counter += 1
        self.name = None
        self.parser = parser

    def __lt__(self, other):
        return self.creation_counter < other.creation_counter


class CommonField:
    def __init__(self, name, parser, is_bit):
        self.name = name
        self.parser = parser
        self.is_bit = is_bit

    def __call__(self, name):
        return self.parser


class MetaClass(type):
    def __new__(cls, name, bases, attrs):
        klass = super().__new__(cls, name, bases, attrs)
        fields = []
        for key, value in attrs.items():
            if isinstance(value, Field):
                value.name = key
                # value.parser = value.parser(value.name) ## construct.UBIntxx(name)
                fields.insert(bisect(fields, value), value)
        klass._parse_order = []
        klass._key_order = []
        bits_fields = []
        for f in fields:
            klass._key_order.append(f.name)
            if isinstance(f.parser(f.name), MBits):
                bits_fields.append(f)
                continue
            elif len(bits_fields) > 0:
                bits_parser = construct.EmbeddedBitStruct(
                    *[f.parser(f.name)._make_con() for f in bits_fields]
                )
                bits_names = ",".join([f.name for f in bits_fields])
                obj = Field(
                    CommonField(name=bits_names, parser=bits_parser, is_bit=True)
                )
                obj.name = bits_names
                bits_fields.clear()
                klass._parse_order.append(obj)
            klass._parse_order.append(f)
        # klass._parse_order = fields
        klass.name = name

        def __str__(self):
            data = []
            for fname in self._key_order:
                value = getattr(self, fname)
                if isinstance(value, list) and len(value) > 5:
                    value = f"[{value[0]},{value[1]},...,{value[-1]}]"
                data.append(f"{fname}={value}")
            return f"{name}({', '.join(data)})"

        def __init__(self, name=name, *args, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
            Construct.__init__(self, name, kwargs.get("flags", 0))  # TODO
            self._consume_num = 0
            # Construct.__init__(self, args[0])

        def __eq__(self, other):
            for key in self._key_order:
                if getattr(self, key, None) != getattr(other, key, None):
                    return False
            return True

        klass.__eq__ = __eq__
        klass.__init__ = __init__
        klass.__str__ = __str__
        klass.__repr__ = __str__
        # klass._parse = _parse
        # klass._build = _build
        # klass.parse_stream = _parse
        return klass


class Struct(Construct, metaclass=MetaClass):
    def build(self, obj=None):
        if obj is None:
            obj = self
        return super().build(obj)

    def _parse(self, stream, context=None):
        start = stream.seek(0, 1)
        for field in self._parse_order:
            try:
                value = field.parser(field.name).parse_stream(stream)
                if getattr(field.parser, "is_bit", None):
                    for key in field.name.split(","):
                        setattr(self, key, getattr(value, key))
                setattr(self, field.name, value)
            except Exception as e:
                logger.error("self %s", self)
                logger.error("parser is %s", field.name)
                raise e
        end = stream.seek(0, 1)
        self._consume_num = end - start

        # self_parse = attrs.get("_parse", None)
        # if self_parse is not None:
        #     self_parse(self, stream, context)

        return self

    def _build(self, obj, stream, context=None):
        for field in self._parse_order:
            field.parser(field.name).build_stream(getattr(obj, field.name), stream)


class ArrayEntry(Construct):
    def __init__(self, subcon):
        self.subcon = subcon
        Construct.__init__(self, "ArrayEntry")

    def _parse(self, stream, context=None):
        s = self.subcon()
        s.parse_stream(stream)
        return s
