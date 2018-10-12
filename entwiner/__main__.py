"""Entwiner CLI."""

import click

from . import database, io


@click.command()
@click.argument("infiles", nargs=-1, type=click.Path("r"))
@click.argument("outfile")
def entwiner(infiles, outfile):
    click.echo("Creating database!")
    db = database.EdgeDB(outfile)
    db.stage()

    click.echo("Importing edges...")
    # Create progress bar(s)
    BATCH_SIZE = 1000
    for path in infiles:
        feature_gen = io.feature_generator(path)
        features = []
        n = 0
        tot = 1
        while True:
            try:
                feature = next(feature_gen)
            except StopIteration as e:
                break

            if n == BATCH_SIZE:
                db.add_edges(features)
                features = []
                n = 0
                tot += BATCH_SIZE
            else:
                features.append(feature)
                n += 1

    db.finalize()

    click.echo("Done!")


if __name__ == "__main__":
    entwiner()
