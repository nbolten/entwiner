"""Entwiner CLI."""
import os
import tempfile

import entwiner
from entwiner.constants import EDGE_BATCH_SIZE


class GraphBuilder:
    def __init__(self, graph_class=entwiner.DiGraphDB, precision=7, changes_sign=None):
        if changes_sign is None:
            changes_sign = []

        self.precision = precision
        self.changes_sign = changes_sign
        self.graph_class = graph_class

        self.G = None

    # TODO: automatic cleanup if this fails.
    def create_temporary_db(self):
        _, path = tempfile.mkstemp()
        G = self.graph_class.create_graph(path)
        self.G = G

    def remove_temporary_db(self):
        if os.path.exists(self.G.sqlitegraph.path):
            os.remove(self.G.sqlitegraph.path)

    def get_G(self):
        return self.G

    def add_edges_from(self, path, batch_size=EDGE_BATCH_SIZE, counter=None):
        edge_gen = entwiner.io.edge_generator(
            path,
            precision=self.precision,
            changes_sign=self.changes_sign,
            add_reverse=True,
        )
        self.G.add_edges_from(edge_gen, _batch_size=batch_size, counter=counter)

    def reindex(self):
        self.G.reindex()
