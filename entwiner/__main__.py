"""Entwiner CLI."""

import click

from . import graphs, io


@click.command()
@click.argument("infiles", nargs=-1, type=click.Path("r"))
@click.argument("outfile")
def entwiner(infiles, outfile):
    click.echo("Creating database!")
    G = graphs.digraphdb.DiGraphDB(database=outfile, create=True)

    click.echo("Importing edges...")
    # Create progress bar(s)
    BATCH_SIZE = 1000
    for path in infiles:
        feature_gen = io.feature_generator(path)
        edge_gen = io.edge_generator(feature_gen)
        G.add_edges_from(edge_gen, _batch_size=BATCH_SIZE)

        # TODO: do this without redundant i/o
        feature_gen_rev = io.feature_generator(path)
        edge_gen_rev = ((e[1], e[0], e[2]) for e in io.edge_generator(feature_gen_rev))
        G.add_edges_from(edge_gen_rev, _batch_size=BATCH_SIZE)

    click.echo("Done!")


if __name__ == "__main__":
    entwiner()
