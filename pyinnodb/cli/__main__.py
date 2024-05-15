import click
from . import static_usage


@click.group()
@click.option("--fn", type=click.File("rb"))
@click.pass_context
def main(ctx, fn):
    ctx.ensure_object(dict)
    ctx.obj["fn"] = fn


main.command()(static_usage.static_page_usage)


if __name__ == "__main__":
    main()
