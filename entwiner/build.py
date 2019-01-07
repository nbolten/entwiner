"""Entwiner build functions - create a graph and populate it from geospatial formats."""

from . import graphs, io


def create_graph(infiles, outfile, precision=7, batch_size=1000):
    G = graphs.digraphdb.DiGraphDB(database=outfile, create=True)

    for path in infiles:
        edge_gen = io.edge_generator(path, precision, rev=True)
        G.add_edges_from(edge_gen, _batch_size=batch_size)

    return G
