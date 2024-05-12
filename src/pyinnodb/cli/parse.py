from . import *
from pyinnodb import disk_struct

@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT)
@click.option("--type", multiple=True, type=click.STRING)
@click.option("--offset", type=click.INT, default=0)
@click.option("--remain", type=click.INT, default=0)
def parse(ctx, pageno, type, offset, remain):
    f = ctx.obj["fn"]
    f.seek(pageno * const.PAGE_SIZE)
    if offset != 0:
        f.seek(offset, 1)
    possible_names = [i for i in dir(disk_struct) if i.startswith("M")]
    for t in type:
        print(t, getattr(disk_struct, t).parse_stream(f))

    print("byte consume is", f.tell() - pageno * const.PAGE_SIZE)
    if remain != 0:
        print("remain:", remain, f.read(remain))
