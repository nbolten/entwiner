"""Entwiner build functions - create a graph and populate it from geospatial formats."""

from . import graphs, io


def create_graph(infiles, outfile):
    G = graphs.digraphdb.DiGraphDB(database=outfile, create=True)

    BATCH_SIZE = 1000
    for path in infiles:
        feature_gen = io.feature_generator(path)
        edge_gen = io.edge_generator(feature_gen)
        G.add_edges_from(edge_gen, _batch_size=BATCH_SIZE)

        # TODO: do this without redundant i/o
        feature_gen_rev = io.feature_generator(path)
        edge_gen_rev = ((e[1], e[0], e[2]) for e in io.edge_generator(feature_gen_rev))
        G.add_edges_from(edge_gen_rev, _batch_size=BATCH_SIZE)

    return G
