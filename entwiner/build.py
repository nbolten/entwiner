import os
import shutil
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
        self.tempfile = None

        self.G = None

    # TODO: automatic cleanup if this fails.
    def create_temporary_db(self):
        # self.G = self.graph_class.create_graph()
        _, path = tempfile.mkstemp()
        path = str(path)
        os.remove(path)
        path = f"{path}.gpkg"
        G = self.graph_class.create_graph(path=path)
        self.tempfile = path
        self.G = G

    def finalize_db(self, path):
        # FIXME: implement proper interface / paradigm for overwriting geopackages
        #        Consider creating path.gpkg.build temporary file

        # TODO: place the rtree step somewhere else?
        self.G.network.edges.add_rtree()
        self.G.network.nodes.add_rtree()

        if os.path.exists(path):
            os.remove(path)
        # self.G.network.copy(path)
        shutil.move(self.tempfile, path)
        self.G.network.gpkg.path = path
        self.tempfile = None

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
