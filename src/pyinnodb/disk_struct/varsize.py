from ..mconstruct import *

class VarSize(CC):
    size : int = cfield(cs.Int8ub)
    
    def _post_parsed(self, stream, context, path):
        if self.size > 0x7F:
            self.size = (self.size - 0x80) * 256 + cs.Int8ub.parse_stream(stream)
