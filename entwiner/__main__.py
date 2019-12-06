"""Entwiner CLI."""
import os

import click
import fiona

from . import build, io, DiGraphDB

BATCH_SIZE = 1000


@click.command()
@click.argument("infiles", nargs=-1, type=click.Path("r"))
@click.argument("outfile")
@click.option("--precision", default=7)
@click.option("--changes-sign", multiple=True)
def entwiner(infiles, outfile, precision, changes_sign):
    click.echo("Creating new graph database... ", nl=False)
    if os.path.exists(outfile):
        os.rename(outfile, outfile + ".bak")
    G = DiGraphDB(path=outfile, create=True)
    click.echo("Done")

    n = 0
    for path in infiles:
        with fiona.open(path) as c:
            n += len(c)

    # Two edges per feature
    n *= 2

    with click.progressbar(length=n, label="Importing edges") as bar:
        for path in infiles:
            edge_gen = io.edge_generator(
                path, precision, rev=True, changes_sign=changes_sign
            )
            G.add_edges_from(edge_gen, _batch_size=BATCH_SIZE, counter=bar)

    click.echo("Creating indices... ", nl=False)
    G.reindex()
    click.echo("Done")

    if os.path.exists(outfile + ".bak"):
        os.remove(outfile + ".bak")


if __name__ == "__main__":
    entwiner()
