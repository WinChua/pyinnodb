from ..mconstruct import *
from datetime import timedelta

class MTime2(CC):
    signed: int = cfield(cs.BitsInteger(1))
    hour: int = cfield(cs.BitsInteger(11))
    minute: int = cfield(cs.BitsInteger(6))
    second: int = cfield(cs.BitsInteger(6))

    def parse_fsp(self, stream, fsp):
        self.fsp = stream.read(fsp)

    def to_timedelta(self) -> timedelta:
        v = timedelta(hours=self.hour, minutes=self.minute, seconds=self.second, microseconds=int.from_bytes(self.fsp))
        return v
