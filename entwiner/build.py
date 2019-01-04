"""Entwiner build functions - create a graph and populate it from geospatial formats."""

from . import graphs, io


def create_graph(infiles, outfile, precision=7, batch_size=1000):
    G = graphs.digraphdb.DiGraphDB(database=outfile, create=True)

    for path in infiles:
        feature_gen = io.feature_generator(path)
        edge_gen = io.edge_generator(feature_gen, precision)
        G.add_edges_from(edge_gen, _batch_size=batch_size)

        # TODO: do this without redundant i/o
        feature_gen_rev = io.feature_generator(path)
        edge_gen_rev = (
            (e[1], e[0], e[2]) for e in io.edge_generator(feature_gen_rev, precision)
        )
        G.add_edges_from(edge_gen_rev, _batch_size=batch_size)

    return G
