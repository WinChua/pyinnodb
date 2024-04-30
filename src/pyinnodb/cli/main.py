import click
import logging
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb import const
from io import BytesIO

import dataclasses

logger = logging.getLogger(__name__)


@click.group()
@click.argument("fn", type=click.File("rb"))
@click.option(
    "--log-level", type=click.Choice(["DEBUG", "ERROR", "INFO"]), default="ERROR"
)
@click.pass_context
def main(ctx, fn, log_level):
    logging.basicConfig(
        format="[%(levelname)s]-[%(filename)s:%(lineno)d] %(message)s", level=log_level
    )
    ctx.ensure_object(dict)
    ctx.obj["fn"] = fn
    fsp_page = MFspPage.parse_stream(fn)
    ctx.obj["fsp_page"] = fsp_page
