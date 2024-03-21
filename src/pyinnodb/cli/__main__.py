import click
from . import static_usage
from . import sql
from pyinnodb.disk_struct.fsp import MFspPage
import logging


@click.group()
@click.argument("fn", type=click.File("rb"))
@click.option(
    "--log-level", type=click.Choice(["DEBUG", "ERROR", "INFO"]), default="ERROR"
)
@click.pass_context
def main(ctx, fn, log_level):
    logging.basicConfig(level=log_level)
    ctx.ensure_object(dict)
    ctx.obj["fn"] = fn
    fsp_page = MFspPage.parse_stream(fn)
    ctx.obj["fsp_page"] = fsp_page
    print(f"MySQL version {fsp_page.fil.pre_page}")


main.command()(static_usage.static_page_usage)
main.command()(sql.tosql)


if __name__ == "__main__":
    main()