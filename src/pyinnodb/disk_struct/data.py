from ..mconstruct import *
from datetime import timedelta, datetime, date
try:
    from datetime import UTC
except:
    from datetime import timezone
    UTC = timezone.utc
import time


class MTime2(CC):
    signed: int = cfield(cs.BitsInteger(1))
    hour: int = cfield(cs.BitsInteger(11))
    minute: int = cfield(cs.BitsInteger(6))
    second: int = cfield(cs.BitsInteger(6))

    def parse_fsp(self, stream, fsp):
        self.fsp = stream.read(fsp)

    def to_timedelta(self) -> timedelta:
        v = timedelta(
            hours=self.hour,
            minutes=self.minute,
            seconds=self.second,
            microseconds=int.from_bytes(self.fsp, "big"),
        )
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
        v = datetime(
            year=int(self.year_month / 13),
            month=self.year_month % 13,
            day=self.day,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=int.from_bytes(self.fsp, "big"),
        )
        return v if self.signed == 1 else -v


class MDate(CC):
    signed: int = cfield(cs.BitsInteger(1))
    year: int = cfield(cs.BitsInteger(14))
    month: int = cfield(cs.BitsInteger(4))
    day: int = cfield(cs.BitsInteger(5))

    def to_date(self) -> date:
        v = date(self.year, self.month, self.day)
        return v if self.signed == 1 else -v


class MTimestamp(CC):
    timestamp: int = cfield(cs.Int32ub)

    def parse_fsp(self, stream, fsp):
        self.fsp = stream.read(fsp)

    def to_time(self) -> datetime:
        return datetime.fromtimestamp(
            self.timestamp + int.from_bytes(self.fsp, "big") / 1000000,
            UTC,
        )

class MPoint(CC):
    x: float = cfield(cs.Float64l)
    y: float = cfield(cs.Float64l)

class MGeo(CC):
    SRID: int = cfield(cs.Int32ub)
    byteorder: int = cfield(cs.Int8ul)
    point_type: int = cfield(cs.Int32ul)

    def _post_parsed(self, stream, context, path):
        if self.point_type == 1: # POINT
            self.x = cs.Float64l.parse_stream(stream)
            self.y = cs.Float64l.parse_stream(stream)
        elif self.point_type == 2: # LINESTRING
            self.points = []
            self.size = cs.Int32ul.parse_stream(stream)
            for i in range(self.size):
                self.points.append(MPoint.parse_stream(stream))

    def _post_build(self, obj, stream, context, path):
        if self.point_type == 1:
            stream.write(cs.Float64l.build(obj.x))
            stream.write(cs.Float64l.build(obj.y))
        elif self.point_type == 2:
            stream.write(cs.Int32ul.build(self.size))
            for p in self.points:
                stream.write(p.build())
