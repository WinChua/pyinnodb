from ..mconstruct import *

class MSmallKeyEntry(CC):
    offset: int = cfield(cs.Int16ul)
    length: int = cfield(cs.Int16ul)

class MKeyEntry(CC):
    offset: int = cfield(cs.Int32ul)
    length: int = cfield(cs.Int16ul)

class MJson(CC):
    type: int = cfield(cs.Int8ul)

    def get_json(self):
        return getattr(self, "_post_value", None)
        

    def _post_parsed(self, stream, context, path):
        if self.type == 0x04:
            data = int.from_bytes(stream.read(1))
            return {1: True, 2: False, 0:None}.get(data, None)
        elif self.type == 0x05:
            return cs.Int16sl.parse_stream(stream)
        elif self.type == 0x06:
            return cs.Int16ul.parse_stream(stream)
        elif self.type == 0x07:
            return cs.Int32sl.parse_stream(stream)
        elif self.type == 0x08:
            return cs.Int32ul.parse_stream(stream)
        elif self.type == 0x09:
            return cs.Int64sl.parse_stream(stream)
        elif self.type == 0x0a:
            return cs.Int64ul.parse_stream(stream)
        elif self.type == 0x0b: # double different when inside object
            return cs.Float64l.parse_stream(stream)
        elif self.type == 0x0c:
            b0 = stream.read(1)
            size = int.from_bytes(b0)
            if size > 0x80:
                b1 = stream.read(1)
                size = int.from_bytes(stream.read(1)) * 128 + (size ^ 0x80)
            return stream.read(size).decode()


        parser = cs.Int16ul if self.type in [0x00, 0x02] else cs.Int32ul
        element_count = parser.parse_stream(stream)
        data_size = parser.parse_stream(stream)

        if self.type in [0x00, 0x01]: # json object
            key_entry = []
            for i in range(element_count):
                offset = parser.parse_stream(stream)
                length = cs.Int16ul.parse_stream(stream)
                key_entry.append((offset, length))

        value_entry = []
        for i in range(element_count):
            eType = stream.peek(1)[0]
            if eType in [0x05,0x06,0x07,0x08,0x09,0x0a]:
                value_entry.append((eType, MJson.parse_stream(stream).get_json()))
            else:
                stream.read(1)
                value_entry.append((eType, parser.parse_stream(stream)))
            pass

        if self.type in [0x00, 0x01]: # json object
            key = []
            for ke in key_entry:
                key.append(stream.read(ke[1]).decode())
                
        value = []
        for ve in value_entry:
            if ve[0] in [0x05, 0x06, 0x07, 0x08, 0x09, 0x0a]:
                value.append(ve[1])
            else:
                value.append(MJson(type=ve[0])._post_parsed(stream, None, None))


        if self.type in [0x00, 0x01]:
            return {k:v for k, v in zip(key, value)}
        return value


        self.spec_size = 2 if self.type in [0x00, 0x02] else 4

        if self.spec_size == 2:
            self.element_count = cs.Int16ul.parse_stream(stream)
            self.data_size = cs.Int16ul.parse_stream(stream)
        else:
            self.element_count = cs.Int32ul.parse_stream(stream)
            self.data_size = cs.Int32ul.parse_stream(stream)

        if self.type in [0x00, 0x01]: # json object
            self.key_entry = []
            for i in range(self.element_count):
                if self.spec_size == 2:
                    self.key_entry.append(MSmallKeyEntry.parse_stream(stream))
                else:
                    self.key_entry.append(MKeyEntry.parse_stream(stream))

