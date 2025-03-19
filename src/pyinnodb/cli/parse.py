from pyinnodb import disk_struct

from . import *


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT, help="pageno number to parse")
@click.option(
    "--type", multiple=True, type=click.STRING, help="struct name to parse in sequence"
)
@click.option("--offset", type=click.INT, default=0, help="page offset to start parse")
@click.option(
    "--remain", type=click.INT, default=0, help="the bytes number to read after parse"
)
@click.option("--func", type=click.STRING, default="")
def parse(ctx, pageno, type, offset, remain, func):
    """explore the ibd file flexibly"""
    f = ctx.obj["fn"]
    f.seek(pageno * const.PAGE_SIZE)
    if offset != 0:
        f.seek(offset, 1)
    possible_names = [i for i in dir(disk_struct) if i.startswith("M")]
    for t in type:
        obj = getattr(disk_struct, t).parse_stream(f)
        print(t, obj)
        if getattr(obj, func, None) is not None:
            print(func, getattr(obj, func, None)())

    print("byte consume is", f.tell() - pageno * const.PAGE_SIZE)
    if remain != 0:
        print("remain:", remain, f.read(remain))
