def line_to_dict(data, linesep, keysep):
    return {k.strip():v.strip() for k, v in [line.strip().split(keysep) for line in data.split(linesep) if line.strip()]}
