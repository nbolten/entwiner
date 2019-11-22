"""Dict-like interface(s) for graphs."""
import sqlite3
import tempfile

import networkx as nx

from ..utils import sqlite_type
from ..sqlitegraph import SQLiteGraph
from ..exceptions import ImmutableGraphError
from .edges import Edge, ImmutableEdge
from .nodes import node_factory_factory, immutable_node_factory_factory

"""
NetworkX classes have been written to allow other dict-like storage methods aside from
the default, which is a plain Python dict. All one needs to do (in theory) is create
a few functions that can be used to generate those dict-like objects - a few factories.

Because our implementation requires storing/retrieving graph information from an SQLite
database, the factories need to have access to a shared database connection.

In other words, we need to embed potentially dynamic information into a factory: we
need factories of factories that take the database connection as an input.

TODO: allow different storage strategies - e.g. pickle or JSON encoding into single
value column rather than spreading keys into columns and requiring flat data.

"""

# FIXME: G._pred is not functioning correctly - need a way to distinguish predecessor
# adjacency dict-like from successor.

# TODO: attach factories to container classes as classmethods
# TODO: consider whether ImmutableEdge is a necessary class. Just use 'dict'? Should
# ImmutableEdge be a collections.abc container?


class ImmutableSuccessors:
    def __init__(self, _sqlitegraph=None):
        self.sqlitegraph = _sqlitegraph

    def items(self):
        # TODO: see note in Successors .items()
        successor_id_generator = self.sqlitegraph.iter_successor_ids()
        return (
            (s_id, ImmutableInnerAdjlist(self.sqlitegraph, s_id, False))
            for s_id in successor_id_generator
        )

    def __getitem__(self, key):
        return ImmutableInnerAdjlist(self.sqlitegraph, key, False)

    def __contains__(self, key):
        return self.sqlitegraph.has_successors(key)

    def __iter__(self):
        return self.sqlitegraph.iter_predecessor_ids()


class Successors(ImmutableSuccessors):
    def clear(self):
        # What should this do? Is it safe to drop all rows given that predecessors
        # might still be defined?
        pass

    def items(self):
        # TODO: investigate whether this method is ever actually used. The atlas views
        # put this container in its ._atlas property and implements its own .items()
        # that actually calls Successors.__getitem__()
        # FIXME: The above is actually very important for routing, as
        # Successors.items() is part of the Dijkstra's method implementation and
        # __getitem__ is relatively slow.
        successor_id_generator = self.sqlitegraph.iter_successor_ids()
        return (
            (s_id, InnerAdjlist(self.sqlitegraph, s_id, False))
            for s_id in successor_id_generator
        )

    def __getitem__(self, key):
        return InnerAdjlist(self.sqlitegraph, key, False)

    def __setitem__(self, key, ddict):
        self.sqlitegraph.replace_successors(key, ((k, v) for k, v in ddict.items))


class ImmutablePredecessors:
    def __init__(self, _sqlitegraph=None):
        self.sqlitegraph = _sqlitegraph

    def items(self):
        self.sqlitegraph.iter_predecessors()

    def __getitem__(self, key):
        return ImmutableInnerAdjlist(self.sqlitegraph, key, True)

    def __contains__(self, key):
        return self.sqlitegraph.has_predecessors(key)

    def __iter__(self):
        self.sqlitegraph.iter_predecessor_ids()


class Predecessors(ImmutablePredecessors):
    def clear(self):
        pass

    def items(self):
        query = self.sqlitegraph.conn.execute("SELECT _v v FROM edges")
        return (
            (row["v"], InnerAdjlist(self.sqlitegraph, row["v"], True)) for row in query
        )

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        return InnerAdjlist(self.sqlitegraph, key, True)

    def __setitem__(self, key, ddict):
        self.sqlitegraph.replace_predecessors(key, ((k, v) for k, v in ddict.items()))


"""Inner adjacency list class + factory."""
# TODO: use Mapping abc for better dict compatibility
class ImmutableInnerAdjlist:
    """Inner adjacency "list": dict-like keyed by neighbors, values are edge
    attributes.

    :param conn: database connection.
    :type conn: sqlite3.Connection
    :param key: Key used to access this adjacency "list" - used for lookups.
    :type key: str
    :param pred: Whether this adjacency list is a "predecessor" list, as opposed to the
                 default of containing successors.
    :type pred: bool
    """

    def __init__(self, _sqlitegraph=None, _key=None, _pred=False):
        self.sqlitegraph = _sqlitegraph
        self.key = _key
        # TODO: point for optimization: remove conditionals on self.pred at
        # initialization
        self.pred = _pred

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def items(self):
        # TODO: avoid predecessor checks by using yet another class like
        # "PredecessorNeighbor"
        if self.pred:
            return self.sqlitegraph.iter_predecessors(self.key)
        else:
            return self.sqlitegraph.iter_successors(self.key)

    def values(self):
        # TODO: check if this is actually immutable - can the Edges be mutated?
        if self.pred:
            edge_ids = self.sqlitegraph.iter_predecessor_ids(self.key)
            return (Edge(self.sqlitegraph, self.key, e) for e in edge_ids)
        else:
            edge_ids = self.sqlitegraph.iter_successor_ids(self.key)
            return (Edge(self.sqlitegraph, e, self.key) for e in edge_ids)

    def __getitem__(self, key):
        if self.pred:
            return self.sqlitegraph.get_edge_attr(key, self.key)
        else:
            return self.sqlitegraph.get_edge_attr(self.key, key)

    def __contains__(self, key):
        if self.pred:
            return self.sqlitegraph.has_edge(key, self.key)
        else:
            return self.sqlitegraph.has_edge(self.key, key)

    def __iter__(self):
        if self.pred:
            return self.sqlitegraph.iter_predecessor_ids(self.key)
        else:
            return self.sqlitegraph.iter_successor_ids(self.key)

    def __len__(self):
        if self.pred:
            query = self.sqlitegraph.conn.execute(
                "SELECT count(*) count FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.sqlitegraph.conn.execute(
                "SELECT count(*) count FROM edges WHERE _u = ?", (self.key,)
            )
        return query.fetchone()["count"]


class InnerAdjlist(ImmutableInnerAdjlist):
    """Inner adjacency "list": dict-like keyed by neighbors, values are edge
    attributes.

    :param conn: database connection.
    :type conn: sqlite3.Connection
    :param key: Key used to access this adjacency "list" - used for lookups.
    :type key: str
    :param pred: Whether this adjacency list is a "predecessor" list, as opposed to the
                 default of containing successors.
    :type pred: bool
    """

    def __init__(self, _sqlitegraph=None, _key=None, _pred=False):
        self.sqlitegraph = _sqlitegraph
        self.key = _key
        # TODO: point for optimization: remove conditionals on self.pred at
        # initialization
        self.pred = _pred

    def items(self):
        if self.pred:
            edge_ids = self.sqlitegraph.iter_predecessor_ids(self.key)
            return ((e, Edge(self.sqlitegraph, self.key, e)) for e in edge_ids)
        else:
            edge_ids = self.sqlitegraph.iter_successor_ids(self.key)
            return ((e, Edge(self.sqlitegraph, e, self.key)) for e in edge_ids)

    def __getitem__(self, key):
        if self.pred:
            has_edge = self.sqlitegraph.has_edge(key, self.key)
        else:
            has_edge = self.sqlitegraph.has_edge(self.key, key)

        if has_edge:
            return Edge(self.sqlitegraph, _u=self.key, _v=key)
        else:
            raise KeyError("No key {}".format(key))

    def __setitem__(self, key, value):
        if key in self:
            if self.pred:
                self.sqlitegraph.update_edge(key, self.key, value)
            else:
                self.sqlitegraph.update_edge(self.key, key, value)
        else:
            if self.pred:
                self.sqlitegraph.add_edge(key, self.key, value)
            else:
                self.sqlitegraph.add_edge(self.key, key, value)

    def values(self):
        if self.pred:
            edge_ids = self.sqlitegraph.iter_predecessor_ids(self.key)
            return (Edge(self.sqlitegraph, self.key, e) for e in edge_ids)
        else:
            edge_ids = self.sqlitegraph.iter_successor_ids(self.key)
            return (Edge(self.sqlitegraph, e, self.key) for e in edge_ids)

    # TODO: implement mutable __iter__


def immutable_predecessors_factory_factory(sqlitegraph):
    def predecessors_factory():
        return ImmutablePredecessors(_sqlitegraph=sqlitegraph)

    return predecessors_factory


def predecessors_factory_factory(sqlitegraph):
    def predecessors_factory():
        return Predecessors(_sqlitegraph=sqlitegraph)

    return predecessors_factory


def immutable_successors_factory_factory(sqlitegraph):
    def successors_factory():
        return ImmutableSuccessors(_sqlitegraph=sqlitegraph)

    return successors_factory


def successors_factory_factory(sqlitegraph):
    def successors_factory():
        return Successors(_sqlitegraph=sqlitegraph)

    return successors_factory


def immutable_adjlist_inner_factory_factory(sqlitegraph):
    def adjlist_inner_factory():
        return ImmutableInnerAdjlist(sqlitegraph)

    return adjlist_inner_factory


def adjlist_inner_factory_factory(sqlitegraph):
    def adjlist_inner_factory():
        return InnerAdjlist(sqlitegraph)

    return adjlist_inner_factory


class DiGraphDB(nx.DiGraph):
    def __init__(
        self,
        incoming_graph_data=None,
        path=None,
        sqlitegraph=None,
        create=False,
        immutable=False,
        in_memory=False,
        **attr
    ):
        if sqlitegraph is None:
            if path is None:
                n, path = tempfile.mkstemp()
            sqlitegraph = SQLiteGraph(path)
        if in_memory:
            sqlitegraph = sqlitegraph.to_in_memory()
        self.sqlitegraph = sqlitegraph
        self.immutable = immutable
        if create:
            self._create()

        # The factories of nx dict-likes need to be informed of the connection
        if immutable:
            self.node_dict_factory = immutable_node_factory_factory(self.sqlitegraph)
            self.adjlist_outer_dict_factory = immutable_successors_factory_factory(
                self.sqlitegraph
            )
            self.adjlist_inner_dict_factory = immutable_adjlist_inner_factory_factory(
                self.sqlitegraph
            )
        else:
            self.node_dict_factory = node_factory_factory(self.sqlitegraph)
            self.adjlist_outer_dict_factory = successors_factory_factory(
                self.sqlitegraph
            )
            self.adjlist_inner_dict_factory = adjlist_inner_factory_factory(
                self.sqlitegraph
            )

        # FIXME: Shouldn't this be 'Edge' or 'ImmutableEdge'?
        self.edge_attr_dict_factory = dict

        # FIXME: should use a persistent table/container for .graph as well.
        self.graph = {}
        self._node = self.node_dict_factory()
        self._adj = self.adjlist_outer_dict_factory()
        if immutable:
            self._pred = immutable_predecessors_factory_factory(self.sqlitegraph)()
        else:
            self._pred = predecessors_factory_factory(self.sqlitegraph)()
        self._succ = self._adj

        if incoming_graph_data is not None:
            nx.convert.to_networkx_graph(incoming_graph_data, create_using=self)
        self.graph.update(attr)

    def add_edges_from(self, ebunch_to_add, _batch_size=10000, **attr):
        """Equivalent to add_edges_from in networkx but with batched SQL writes.

        :param ebunch_to_add: edge bunch, identical to nx ebunch_to_add.
        :type ebunch_to_add: edge bunch
        :param _batch_size: Number of rows to commit to the database at a time.
        :type _batch_size: int
        :param attr: Default attributes, identical to nx attr.
        :type attr:

        """
        self._check_immutable()

        if _batch_size < 2:
            # User has entered invalid number (negative, zero) or 1. Use default behavior.
            super().add_edges_from(self, ebunch_to_add, **attr)
            return

        self.sqlitegraph.add_edges_batched(ebunch_to_add, _batch_size)

    def size(self, weight=None):
        if weight is None:
            query_results = self.sqlitegraph.execute("SELECT count() FROM edges")
            count = next(query_results)["count()"]
            return count
        else:
            return super().size(weight=weight)

    def iter_edges(self):
        """Roughly equivalent to the .edges interface, but much faster.

        :returns: generator of (u, v, d) similar to .edges, but where d is a
                  dictionary, not an Edge that syncs to database.
        :rtype: tuple generator

        """
        # FIXME: this is currently a read-only strategy (data is converted to dict).
        # We should offer a read-only and non-read-only version downstream.
        return self.sqlitegraph.iter_edges()

    def update_edges(self, ebunch):
        return self.sqlitegraph.update_edges(ebunch)

    def reindex(self):
        self.sqlitegraph.reindex()

    def _create(self):
        self._check_immutable()
        self.sqlitegraph._create_graph()

    def _check_immutable(self):
        if self.immutable:
            raise ImmutableGraphError("Attempted to modify read-only/immutable graph.")
