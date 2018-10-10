"""Entwiner CLI."""

import click

from . import database, io


@click.command()
@click.argument("infiles", nargs=-1, type=click.Path("r"))
@click.argument("outfile")
def entwiner(infiles, outfile):
    click.echo("Creating database!")
    db = database.initialize_db(outfile)

    click.echo("Importing edges...")
    # Create progress bar(s)
    for path in infiles:
        for feature in io.feature_generator(path):
            database.add_edge(db, feature)

    database.finalize_db(outfile)

    click.echo("Done!")


if __name__ == "__main__":
    entwiner()
