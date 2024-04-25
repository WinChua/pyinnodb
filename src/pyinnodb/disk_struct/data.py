from ..mconstruct import *
from datetime import timedelta, datetime

class MTime2(CC):
    signed: int = cfield(cs.BitsInteger(1))
    hour: int = cfield(cs.BitsInteger(11))
    minute: int = cfield(cs.BitsInteger(6))
    second: int = cfield(cs.BitsInteger(6))

    def parse_fsp(self, stream, fsp):
        self.fsp = stream.read(fsp)

    def to_timedelta(self) -> timedelta:
        v = timedelta(hours=self.hour, minutes=self.minute, seconds=self.second, microseconds=int.from_bytes(self.fsp))
        return v if self.signed == 1 else -v

class MDatetime(CC):
    signed: int = cfield(cs.BitsInteger(1))
    year_month: int = cfield(cs.BitsInteger(17))
    day: int = cfield(cs.BitsInteger(5))
    hour: int = cfield(cs.BitsInteger(5))
    minute: int = cfield(cs.BitsInteger(6))
    second: int = cfield(cs.BitsInteger(6))

    def parse_fsp(self, stream, fsp):
        self.fsp = stream.read(fsp)

    def to_datetime(self) -> datetime:
        return datetime(year=int(self.year_month/13), month=self.year_month % 13, day=self.day, hour=self.hour,
                minute = self.minute, second=self.second, microsecond=int.from_bytes(self.fsp))
        pass
