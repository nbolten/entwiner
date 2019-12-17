"""Entwiner CLI."""
import os

import click
import fiona

from entwiner import GraphBuilder
import entwiner


@click.command()
@click.argument("infiles", nargs=-1, type=click.Path("r"))
@click.argument("outfile")
@click.option("--precision", default=7)
@click.option("--changes-sign", multiple=True)
def entwiner(infiles, outfile, precision, changes_sign):
    #
    # Initialize temporary database. If build fails, original remains untouched.
    # TODO: make this behavior user-configurable: graph DBs might become very large,
    # warranting a delete-first approach?
    #
    click.echo("Creating new graph database... ", nl=False)
    builder = GraphBuilder(precision=precision, changes_sign=changes_sign)
    builder.create_temporary_db()
    click.echo("Done")

    #
    # Estimate number of edges to add -
    #
    n = 0
    for path in infiles:
        with fiona.open(path) as c:
            n += len(c)
    # Two edges per feature
    n *= 2

    #
    # Process and import edges into graph db
    #
    with click.progressbar(length=n, label="Importing edges") as bar:
        for path in infiles:
            builder.add_edges_from(path, counter=bar)

    #
    # Reindex, as indices may have been dropped during import.
    #
    click.echo("Creating indices... ", nl=False)
    builder.reindex

    builder.finalize_db(outfile)

    click.echo("Done")
