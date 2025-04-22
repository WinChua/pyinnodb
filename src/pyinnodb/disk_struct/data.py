from datetime import timedelta, datetime, date
from ..mconstruct import cs, cfield, CC

try:
    from datetime import UTC
except:
    from datetime import timezone

    UTC = timezone.utc

TIMEF_INT_OFS = 0x800000
TIMEF_OFS = 0x800000000000

def long2time(hms, dec):
    neg = ""
    if hms < 0:
        neg = "-"
        hms = -hms
    tmp = hms >> 24
    hour = (tmp >> 12) % (1<<10)
    minute = (tmp>> 6) % (1<<6)
    second = (tmp) % (1<<6)
    frac = (hms) % (1<<24)
    if dec == 5:
        frac //= 10
    if dec != 0:
        return f"'{neg}{hour:02}:{minute:02}:{second:02}.{frac:0{dec}}'"
    else:
        return f"'{neg}{hour:02}:{minute:02}:{second:02}'"

class MTime2(CC):
    bin_data: int = cfield(cs.Bytes(3))

    def set_precision(self, dec):
        self.dec = dec
        
    def to_str(self):
        if self.dec in [5, 6]:
            int_part = self.bin_data
            total = int.from_bytes(int_part + self.fsp, "big") - TIMEF_OFS
            return long2time(total, self.dec)
        if self.dec == 0:
            int_part = cs.Int24ub.parse(self.bin_data) - TIMEF_INT_OFS
            if int_part > (1<<23):
                int_part -= (1<<24)
            return long2time(int_part<<24, self.dec)
        elif self.dec in [1, 2]:
            int_part = cs.Int24ub.parse(self.bin_data) - TIMEF_INT_OFS
            frac_part = cs.Int8ub.parse(self.fsp)
            if int_part < 0 and frac_part != 0:
                int_part += 1
                frac_part -= 0x100
            if int_part > (1<<23):
                int_part -= (1<<24)
            if self.dec == 1:
                frac_part //= 10
            return long2time((int_part << 24) + frac_part, self.dec)
        elif self.dec in [3, 4]:
            int_part = cs.Int24ub.parse(self.bin_data) - TIMEF_INT_OFS
            frac_part = cs.Int16ub.parse(self.fsp)
            if int_part < 0 and frac_part != 0:
                int_part += 1
                frac_part -= 0x10000
            if int_part > (1<<23):
                int_part -= (1<<24)
            if self.dec == 3:
                frac_part //= 10
            return long2time((int_part << 24) + frac_part, self.dec)
    
    def parse_fsp(self, stream, fsp):
        self.fsp = stream.read(fsp)


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


class MGPoint(CC):
    byteorder: int = cfield(cs.Int8ul)
    point_type: int = cfield(cs.Int32ul)

    def _post_parsed(self, stream, context, path):
        if self.point_type == 1:  # POINT
            self._set_show_field("x", cs.Float64l.parse_stream(stream))
            self._set_show_field("y", cs.Float64l.parse_stream(stream))
        elif self.point_type == 2:  # LINESTRING
            self._set_show_field("size", cs.Int32ul.parse_stream(stream))
            self._set_show_field("points", [])
            for i in range(self.size):
                self.points.append(MPoint.parse_stream(stream))
        elif self.point_type == 3:  # POLYGON
            self._set_show_field("psize", cs.Int32ul.parse_stream(stream))
            self._set_show_field("polygon", [])
            for i in range(self.psize):
                size = cs.Int32ul.parse_stream(stream)
                points = []
                for j in range(size):
                    points.append(MPoint.parse_stream(stream))
                self.polygon.append(points)
        elif self.point_type == 4:  # MULTIPOINT
            self._set_show_field("size", cs.Int32ul.parse_stream(stream))
            self._set_show_field("points", [])
            for i in range(self.size):
                self.points.append(MGPoint.parse_stream(stream))
        elif self.point_type == 5:
            self._set_show_field("size", cs.Int32ul.parse_stream(stream))
            self._set_show_field("lines", [])
            for i in range(self.size):
                self.lines.append(MGPoint.parse_stream(stream))
        elif self.point_type == 6:
            self._set_show_field("size", cs.Int32ul.parse_stream(stream))
            self._set_show_field("polygons", [])
            for i in range(self.size):
                self.polygons.append(MGPoint.parse_stream(stream))

        elif self.point_type == 7:
            self._set_show_field("size", cs.Int32ul.parse_stream(stream))
            self._set_show_field("geos", [])
            for i in range(self.size):
                self.geos.append(MGPoint.parse_stream(stream))

    def _post_build(self, obj, stream, context, path):
        if self.point_type == 1:
            stream.write(cs.Float64l.build(obj.x))
            stream.write(cs.Float64l.build(obj.y))
        elif self.point_type == 2:
            stream.write(cs.Int32ul.build(self.size))
            for p in self.points:
                stream.write(p.build())
        elif self.point_type == 3:
            stream.write(cs.Int32ul.build(self.psize))
            for polygon in self.polygon:
                if len(polygon) == 0:
                    continue
                stream.write(cs.Int32ul.build(len(polygon)))
                for p in polygon:
                    stream.write(p.build())

        elif self.point_type == 4:
            stream.write(cs.Int32ul.build(self.size))
            for p in self.points:
                stream.write(p.build())
        elif self.point_type == 5:
            stream.write(cs.Int32ul.build(self.size))
            for l in self.lines:
                stream.write(l.build())

        elif self.point_type == 6:
            stream.write(cs.Int32ul.build(self.size))
            for l in self.polygons:
                stream.write(l.build())

        elif self.point_type == 7:
            stream.write(cs.Int32ul.build(self.size))
            for l in self.geos:
                stream.write(l.build())


class MGeo(CC):
    SRID: int = cfield(cs.Int32ub)
    GP: MGPoint = cfield(MGPoint)
