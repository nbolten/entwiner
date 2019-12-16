"""Dict-like interface(s) for graphs."""
from functools import partial
import os

import networkx as nx

from ..sqlitegraph import SQLiteGraph
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

    def __init__(self, incoming_graph_data=None, path=None, sqlitegraph=None, **attr):
        # Path attr overrides sqlite attr
        if path:
            sqlitegraph = SQLiteGraph(path)

        self.sqlitegraph = sqlitegraph

        # The factories of nx dict-likes need to be informed of the connection
        self.node_dict_factory = partial(
            self.node_dict_factory, _sqlitegraph=sqlitegraph
        )
        self.adjlist_outer_dict_factory = partial(
            self.adjlist_outer_dict_factory, _sqlitegraph=sqlitegraph
        )
        self.adjlist_inner_dict_factory = self.adjlist_inner_dict_factory
        self.edge_attr_dict_factory = partial(
            self.edge_attr_dict_factory, _sqlitegraph=sqlitegraph
        )

        # FIXME: should use a persistent table/container for .graph as well.
        self.graph = {}
        self._node = self.node_dict_factory()
        self._succ = self._adj = self.adjlist_outer_dict_factory()
        self._pred = OuterPredecessorsView(_sqlitegraph=self.sqlitegraph)

        if incoming_graph_data is not None:
            nx.convert.to_networkx_graph(incoming_graph_data, create_using=self)
        self.graph.update(attr)

        # Set custom flag for read-only graph DBs
        self.mutable = False

    def size(self, weight=None):
        if weight is None:
            return self.sqlitegraph.len_edges()
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
            for u, v, d in self.sqlitegraph.iter_edges()
        )

    def edges_dwithin(self, lon, lat, distance, sort=False):
        return self.sqlitegraph.edges_dwithin(lon, lat, distance, sort=sort)

    def to_in_memory(self):
        new_sqlitegraph = self.sqlitegraph.to_in_memory()
        return self.__class__(sqlitegraph=new_sqlitegraph)


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

    def __init__(self, *args, path=None, sqlitegraph=None, **kwargs):
        # TODO: Consider adding database file existence checker rather than always
        # checking on initialization?
        if sqlitegraph is None:
            if path is None:
                raise UnderSpecifiedGraphError()
            else:
                if not os.path.exists(path):
                    raise UnderSpecifiedGraphError(
                        "DB file does not exist. Consider using DiGraphDB.create_graph"
                    )

                sqlitegraph = SQLiteGraph(path)

        super().__init__(*args, path=path, sqlitegraph=sqlitegraph, **kwargs)
        self.mutable = False

    @classmethod
    def create_graph(cls, path, *args, **kwargs):
        sqlitegraph = SQLiteGraph(path)
        sqlitegraph._create_graph()
        return DiGraphDB(sqlitegraph=sqlitegraph, *args, **kwargs)

    def add_edges_from(self, ebunch_to_add, _batch_size=1000, counter=None, **attr):
        """Equivalent to add_edges_from in networkx but with batched SQL writes.

        :param ebunch_to_add: edge bunch, identical to nx ebunch_to_add.
        :type ebunch_to_add: edge bunch
        :param _batch_size: Number of rows to commit to the database at a time.
        :type _batch_size: int
        :param attr: Default attributes, identical to nx attr.
        :type attr:

        """
        if _batch_size < 2:
            # User has entered invalid number (negative, zero) or 1. Use default behavior.
            super().add_edges_from(self, ebunch_to_add, **attr)
            return

        self.sqlitegraph.add_edges_batched(ebunch_to_add, _batch_size, counter=counter)

    def update_edges(self, ebunch):
        return self.sqlitegraph.update_edges(ebunch)

    def reindex(self):
        self.sqlitegraph.reindex()
