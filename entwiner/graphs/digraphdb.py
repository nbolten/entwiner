"""Dict-like interface(s) for graphs."""
from functools import partial
import os
import uuid

import networkx as nx

from ..sqlitegraph import SQLiteGraph
from ..geopackagenetwork import GeoPackageNetwork
from ..exceptions import ImmutableGraphError, UnderspecifiedGraphError
from .edges import Edge, EdgeView
from .nodes import Nodes, NodesView
from .outer_adjlists import OuterPredecessors, OuterSuccessors
from .outer_adjlists import OuterPredecessorsView, OuterSuccessorsView
from .inner_adjlists import InnerPredecessors, InnerSuccessors
from .inner_adjlists import InnerPredecessorsView, InnerSuccessorsView


class DiGraphDBView(nx.DiGraph):
    node_dict_factory = NodesView
    adjlist_outer_dict_factory = OuterSuccessorsView
    # In networkx, inner adjlist is only ever invoked without parameters in
    # order to assign new nodes or edges with no attr. Therefore, its functionality
    # can be accounted for elsewhere: via __getitem__ and __setitem__ on the
    # outer adjacency list.
    adjlist_inner_dict_factory = dict
    edge_attr_dict_factory = EdgeView

    def __init__(self, incoming_graph_data=None, path=None, network=None, **attr):
        # Path attr overrides sqlite attr
        if path:
            network = GeoPackageNetwork(path)

        self.network = network

        # The factories of nx dict-likes need to be informed of the connection
        self.node_dict_factory = partial(
            self.node_dict_factory, _network=self.network
        )
        self.adjlist_outer_dict_factory = partial(
            self.adjlist_outer_dict_factory, _network=self.network
        )
        self.adjlist_inner_dict_factory = self.adjlist_inner_dict_factory
        self.edge_attr_dict_factory = partial(
            self.edge_attr_dict_factory, _network=self.network
        )

        # FIXME: should use a persistent table/container for .graph as well.
        self.graph = {}
        self._node = self.node_dict_factory()
        self._succ = self._adj = self.adjlist_outer_dict_factory()
        self._pred = OuterPredecessorsView(_network=self.network)

        if incoming_graph_data is not None:
            nx.convert.to_networkx_graph(incoming_graph_data, create_using=self)
        self.graph.update(attr)

        # Set custom flag for read-only graph DBs
        self.mutable = False

    def size(self, weight=None):
        if weight is None:
            return len(self.network.edges)
        else:
            return super().size(weight=weight)

    def iter_edges(self):
        """Roughly equivalent to the .edges interface, but much faster.

        :returns: generator of (u, v, d) similar to .edges, but where d is a
                  dictionary, not an Edge that syncs to database.
        :rtype: tuple generator

        """
        return (
            (u, v, self.edge_attr_dict_factory(_u=u, _v=v, **d))
            for u, v, d in self.network.edges
        )

    def edges_dwithin(self, lon, lat, distance, sort=False):
        # TODO: document self.network.edges instead?
        return self.network.edges.dwithin(lon, lat, distance, sort=sort)

    def to_in_memory(self):
        # TODO: make into 'copy' method instead, taking path as a parameter?
        db_id = uuid.uuid4()
        path = f"file:entwiner-{db_id}?mode=memory&cache=shared"
        new_network = self.network.copy(path)
        return self.__class__(network=new_network)


class DiGraphDB(DiGraphDBView):
    """Read-only (immutable) version of DiGraphDB.
    :param args: Positional arguments compatible with networkx.DiGraph.
    :type args: array-like
    :param path: An optional path to database file (or :memory:-type string).
    :type path: str
    :param sqlitegraph: An optional path to a custom SQLiteGraph instance.
    :type sqlitegraph: SQLiteGraph
    :param kwargs: Keyword arguments compatible with networkx.DiGraph.
    :type kwargs: dict-like
    """

    node_dict_factory = Nodes
    adjlist_outer_dict_factory = OuterSuccessors
    # TODO: consider creating a read-only Mapping in the case of immutable graphs.
    adjlist_inner_dict_factory = dict
    edge_attr_dict_factory = Edge

    def __init__(self, *args, path=None, network=None, **kwargs):
        # TODO: Consider adding database file existence checker rather than always
        # checking on initialization?
        if network is None:
            # FIXME: should path be allowed to be None?
            if path is None:
                raise UnderspecifiedGraphError()
            else:
                if not os.path.exists(path):
                    raise UnderspecifiedGraphError(
                        "DB file does not exist. Consider using DiGraphDB.create_graph"
                    )

                network = GeoPackageNetwork(path)

        super().__init__(*args, path=path, network=network, **kwargs)
        self.mutable = True

    def iter_edges(self):
        """Roughly equivalent to the .edges interface, but much faster.

        :returns: generator of (u, v, d) similar to .edges, but where d is a
                  dictionary, not an Edge that syncs to database.
        :rtype: tuple generator

        """
        # TODO: investigate performance of iterating over self.network.edges vs.
        # a dedicated u-v only iterator.
        return (
            (u, v, self.edge_attr_dict_factory(_u=u, _v=v))
            for u, v, d in self.network.edges
        )

    @classmethod
    def create_graph(cls, *args, path=None, **kwargs):
        network = GeoPackageNetwork(path)
        return DiGraphDB(network=network, *args, **kwargs)

    def add_edges_from(self, ebunch, _batch_size=1000, counter=None, **attr):
        """Equivalent to add_edges_from in networkx but with batched SQL writes.

        :param ebunch: edge bunch, identical to nx ebunch_to_add.
        :type ebunch: edge bunch
        :param _batch_size: Number of rows to commit to the database at a time.
        :type _batch_size: int
        :param attr: Default attributes, identical to nx attr.
        :type attr:

        """
        if _batch_size < 2:
            # User has entered invalid number (negative, zero) or 1. Use default behavior.
            super().add_edges_from(self, ebunch_to_add, **attr)
            return

        # TODO: length check on each edge
        features = ({"_u": edge[0], "_v": edge[1], **edge[2]} for edge in ebunch)
        self.network.edges.write_features(features, batch_size=_batch_size, counter=counter)

    def update_edges(self, ebunch):
        # FIXME: this doesn't actually work. Implement update / upsert logic for
        # GeoPackage feature tables, then use that.
        return self.network.edges.update(ebunch)
