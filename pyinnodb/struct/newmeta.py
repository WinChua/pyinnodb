from bisect import bisect
from io import BytesIO
import types

from elftools import construct
from elftools.construct import Construct
import logging

logger = logging.Logger(__name__)


def BitArray(size, length):
    return construct.Bitwise(construct.Array(size, construct.Bits("", length)))

class AttrNameCall(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, name):
        return self.func(name)

String = lambda size: AttrNameCall(lambda name: construct.String(name, size))
UBInt8 = AttrNameCall(lambda name: OrderFormatField(name, ">", "B"))
UBInt16 = AttrNameCall(lambda name: OrderFormatField(name, ">", "H"))
UBInt32 = AttrNameCall(lambda name: OrderFormatField(name, ">", "L"))
UBInt64 = AttrNameCall(lambda name: OrderFormatField(name, ">", "Q"))
OBits = lambda length: AttrNameCall(lambda name: OMBits(name, length))

SBInt8 = AttrNameCall(lambda name: OrderFormatField(name, ">", "b"))
SBInt16= AttrNameCall(lambda name: OrderFormatField(name, ">", "h"))
SBInt32= AttrNameCall(lambda name: OrderFormatField(name, ">", "l"))
SBInt64= AttrNameCall(lambda name: OrderFormatField(name, ">", "q"))

class Array(Construct):
    def __init__(self, size, constructor):
        Construct.__init__(self, "", 0)
        self.size = size
        self.constructor = constructor

    def _parse(self, stream, context=None):
        result = []
        print("construct is ", self.constructor)
        for i in range(self.size):
            v = self.constructor.parse_stream(stream)
            print("value is v", v)
            result.append(v)
        return result

    def _build(self, obj, stream, context=None):
        for e in obj:
            self.constructor.build_stream(e, stream)

class intfrombytes(construct.Construct):
    def __init__(self, length):
        self.length = length

    def _parse(self, stream, context=None):
        return int.from_bytes(stream.read(self.length), "big")

    def _build(self, obj, stream, context=None):
        v = int.to_bytes(obj, self.length, "big")
        stream.write(v)


class OMBits:
    def __init__(self, name, length):
        self.name = name
        self.length = length

    def _make_con(self):
        return construct.Bits(self.name, self.length)


class ostruct(Construct):
    pass


class OrderParseMeta(type):
    def __new__(
        cls, name, bases, attrs
    ):  ## after py3.7 key of dict is iterated in insert order
        klass = super().__new__(cls, name, bases, attrs)
        keys_order = []
        parser_fields = []
        bits_fields = []
        for attr_name, parser in attrs.items():
            if isinstance(parser, AttrNameCall):
                parser = parser(attr_name)

            if isinstance(parser, OMBits):
                bits_fields.append((attr_name, parser))
                keys_order.append(attr_name)
                continue
            elif len(bits_fields) > 0:
                bits_parser = construct.EmbeddedBitStruct(
                    *[f[1]._make_con() for f in bits_fields]    
                )
                parser_fields.append(("", bits_parser))
                bits_fields = []
            if isinstance(parser, construct.Construct) or (
                isinstance(parser, type) and issubclass(parser, ostruct)
            ):  ## for class that we define
                keys_order.append(attr_name)
                parser_fields.append((attr_name, parser))

        # parser_fields.sort(key = lambda x: x[1])
        if len(bits_fields) > 0:
            bits_parser = construct.EmbeddedBitStruct(
                *[f[1]._make_con() for f in bits_fields]    
            )
            parser_fields.append(("", bits_parser))
            bits_fields = []

        klass._attr_order = keys_order
        klass._parse_order = []
        for f in parser_fields:
            klass._parse_order.append(f)

        def __str__(self):
            data = []
            for f in self._attr_order:
                value = getattr(self, f)
                if isinstance(value, list) and len(value) > 5:
                    value = f"[{value[0]},{value[1]},...,{value[-1]}]"
                data.append(f"{f}={value}")
            return f"{name}({', '.join(data)})"

        def __eq__(self, other):
            for p in self._attr_order:
                if getattr(self, p, None) != getattr(other, p, None):
                    return False

            return True

        def _repr_html_(self):
            data = []
            for f in self._attr_order:
                value = getattr(self, f)
                s = str(value)
                if hasattr(value, "_repr_html_"):
                    s = value._repr_html_()
                if isinstance(value, list) and len(value) > 5:
                    s = f"[{value[0]},{value[1]},...,{value[-1]}]"
                data.append("<tr><td>"+f+"</td><td>"+s+"</td></tr>")
            return "<table>"+'\n'.join(data)+"</table>"
        klass._repr_html_ = _repr_html_
        klass.__eq__ = __eq__
        klass.__str__ = __str__
        klass.__repr__ = __str__
        return klass


class OStruct(ostruct, metaclass=OrderParseMeta):
    def __init__(self, **kwargs):
        Construct.__init__(self, "", 0)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def build(self, obj=None):
        if obj is None:
            obj = self
        return super().build(obj)

    @classmethod
    def _parse(cls, stream, context=None):
        self = cls()
        start = stream.seek(0, 1)
        for field in cls._parse_order:
            attr_name = field[0]
            parser = field[1]
            value = parser.parse_stream(stream)
            if isinstance(value, construct.Container):
                for k in value.keys():
                    setattr(self, k, value[k])
            else:
                setattr(self, attr_name, value)
        end = stream.seek(0, 1)
        self._consume_num = end - start

        # self_parse = attrs.get("_parse", None)
        # if self_parse is not None:
        #     self_parse(self, stream, context)

        return self

    @classmethod
    def parse(cls, data):
        return cls.parse_stream(BytesIO(data))

    @classmethod
    def parse_stream(cls, stream, context=None):
        return cls._parse(stream, context)

    def _build(self, obj, stream, context=None):
        for field in self._parse_order:
            try:
                value = getattr(obj, field[0])
                logger.info("value is %s, field is %s", value, field)
                if isinstance(field[1], construct.Construct):
                    field[1].build_stream(value, stream)
                else:
                    value.build_stream(value, stream)

            except Exception as e:
                print("the field is", field, value)
                raise e


class OrderFormatField(construct.FormatField):
    pass


class TestOStruct(OStruct):
    a = UBInt8("a")
    b = UBInt16("b")


class TestOStruct2(OStruct):
    e = TestOStruct
    c = UBInt8("c")


class Test(OStruct):
    e = TestOStruct
    d = UBInt8

    @classmethod
    def hello(cls):
        return cls()


if __name__ == "__main__":
    stream = BytesIO(b"\x01\x02\x03")
    print(TestOStruct.parse_stream(stream))
    print(TestOStruct.parse(b"\x01\x02\x03"))
    print(TestOStruct2.parse(b"\x01\x02\x03\x04"))
    a = TestOStruct2.parse(b"\x01\x01\x02\x04")
    print(TestOStruct.parse(b"\x01\x02\x04") == TestOStruct.parse(b"\x01\x02\x04"))
    print(Test.parse(b"\x01\x02\x03\x04"))
