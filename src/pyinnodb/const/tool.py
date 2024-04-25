import construct



def line_to_dict(data, linesep, keysep):
    return {
        k.strip(): v.strip()
        for k, v in [
            line.strip().split(keysep) for line in data.split(linesep) if line.strip()
        ]
    }


def parse_var_size(stream):
    stream.seek(-1, 1)
    size = construct.Int8ub.parse_stream(stream)
    stream.seek(-1, 1)
    if size > 0x7F:
        stream.seek(-1, 1)
        parts = construct.Int8ub.parse_stream(stream)
        stream.seek(-1, 1)
        return (size-0x80)*256 + parts
    return size
