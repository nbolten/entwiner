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
database, the factories need to have access to a shared database connection (the
:memory: SQLite option creates a new db each time - we need to share a single
connection).

In other words, we need to embed potentially dynamic information into a factory: we
need factories of factories that take the database connection as an input.

TODO: allow different storage strategies - e.g. pickle or JSON encoding into single
value column rather than spreading keys into columns and requiring flat data.

"""

# FIXME: G._pred is not functioning correctly - need a way to distinguish predecessor
# adjacency dict-like from successor.


class Successors:
    def __init__(self, _sqlitegraph=None, _immutable=False):
        self.sqlitegraph = _sqlitegraph
        self.immutable = _immutable

    def clear(self):
        # What should this do? Is it safe to drop all rows given that predecessors
        # might still be defined?
        pass

    def items(self):
        successor_id_generator = self.sqlitegraph.iter_successor_ids()
        return (
            (s_id, InnerAdjlist(self.sqlitegraph, s_id, False, self.immutable))
            for s_id in successor_id_generator
        )

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        return InnerAdjlist(self.sqlitegraph, key, False, self.immutable)

    def __contains__(self, key):
        return self.sqlitegraph.has_successors(key)

    def __iter__(self):
        return self.sqlitegraph.iter_predecessor_ids()

    def __setitem__(self, key, ddict):
        if self.immutable:
            raise ImmutableGraphError
        self.sqlitegraph.replace_successors(key, ((k, v) for k, v in ddict.items))


class Predecessors:
    def __init__(self, _sqlitegraph=None, _immutable=False):
        self.sqlitegraph = _sqlitegraph
        self.immutable = _immutable

    def clear(self):
        pass

    def items(self):
        query = self.sqlitegraph.conn.execute("SELECT _v FROM edges")
        return (
            (row[0], InnerAdjlist(self.sqlitegraph, row[0], True, self.immutable))
            for row in query
        )

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        return InnerAdjlist(self.sqlitegraph, key, True, self.immutable)

    def __contains__(self, key):
        return self.sqlitegraph.has_predecessors(key)

    def __iter__(self):
        query = self.sqlitegraph.conn.execute("SELECT DISTINCT _v FROM edges")
        return (row[0] for row in query)

    def __setitem__(self, key, ddict):
        if self.immutable:
            raise ImmutableGraphError
        self.sqlitegraph.replace_predecessors(key, ((k, v) for k, v in ddict.items()))


def predecessors_factory_factory(sqlitegraph, immutable=False):
    def predecessors_factory():
        return Predecessors(_sqlitegraph=sqlitegraph, _immutable=immutable)

    return predecessors_factory


def successors_factory_factory(sqlitegraph, immutable=False):
    def successors_factory():
        return Successors(_sqlitegraph=sqlitegraph, _immutable=immutable)

    return successors_factory


"""Inner adjacency list class + factory."""
# TODO: use Mapping abc for better dict compatibility
class InnerAdjlist:
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

    def __init__(self, _sqlitegraph=None, _key=None, _pred=False, _immutable=False):
        self.sqlitegraph = _sqlitegraph
        self.key = _key
        # TODO: point for optimization: remove conditionals on self.pred at
        # initialization
        self.pred = _pred
        if _immutable:
            self.edge_class = ImmutableEdge
        else:
            self.edge_class = Edge

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def items(self):
        # TODO: point for optimization for read-only classes. Should do lookup at the
        # same time as finding edge IDs.
        if self.pred:
            edge_ids = self.sqlitegraph.iter_predecessor_ids(self.key)
            return (
                (e, self.edge_class(self.sqlitegraph, self.key, e)) for e in edge_ids
            )
        else:
            edge_ids = self.sqlitegraph.iter_successor_ids(self.key)
            return (
                (e, self.edge_class(self.sqlitegraph, e, self.key)) for e in edge_ids
            )

    def __getitem__(self, key):
        if self.pred:
            query = self.sqlitegraph.conn.execute(
                "SELECT * FROM edges WHERE _u = ? AND _v = ?", (key, self.key)
            )
        else:
            query = self.sqlitegraph.conn.execute(
                "SELECT * FROM edges WHERE _u = ? AND _v = ?", (self.key, key)
            )

        if query.fetchone() is not None:
            return self.edge_class(self.sqlitegraph, _u=self.key, _v=key)
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

    def __contains__(self, key):
        if self.pred:
            query = self.sqlitegraph.conn.execute(
                "SELECT * FROM edges WHERE _v = ?", (key,)
            )
        else:
            query = self.grpahdb.conn.execute(
                "SELECT * FROM edges WHERE _u = ?", (key,)
            )
        try:
            next(query)
        except:
            return False
        return True

    def __iter__(self):
        if self.pred:
            query = self.sqlitegraph.conn.execute(
                "SELECT _u FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.sqlitegraph.conn.execute(
                "SELECT _v FROM edges WHERE _u = ?", (self.key,)
            )
        return (row[0] for row in query)

    def __len__(self):
        if self.pred:
            query = self.sqlitegraph.conn.execute(
                "SELECT count(*) FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.sqlitegraph.conn.execute(
                "SELECT count(*) FROM edges WHERE _u = ?", (self.key,)
            )
        return query.fetchone()[0]


def adjlist_inner_factory_factory(sqlitegraph, immutable=False):
    def adjlist_inner_factory():
        return InnerAdjlist(sqlitegraph, _immutable=False)

    def immutable_adjlist_inner_factory():
        return InnerAdjlist(sqlitegraph, _immutable=True)

    if immutable:
        return immutable_adjlist_inner_factory
    else:
        return adjlist_inner_factory


class DiGraphDB(nx.DiGraph):
    def __init__(
        self,
        incoming_graph_data=None,
        path=None,
        sqlitegraph=None,
        create=False,
        immutable=False,
        **attr
    ):
        if sqlitegraph is None:
            if path is None:
                n, path = tempfile.mkstemp()
            sqlitegraph = SQLiteGraph(path)
        self.sqlitegraph = sqlitegraph
        self.immutable = immutable
        if create:
            self._create()

        # The factories of nx dict-likes need to be informed of the connection
        if immutable:
            self.node_dict_factory = immutable_node_factory_factory(self.sqlitegraph)
        else:
            self.node_dict_factory = node_factory_factory(self.sqlitegraph)
        self.adjlist_outer_dict_factory = successors_factory_factory(
            self.sqlitegraph, immutable=immutable
        )
        self.adjlist_inner_dict_factory = adjlist_inner_factory_factory(
            self.sqlitegraph, immutable=immutable
        )
        self.edge_attr_dict_factory = dict

        # FIXME: should use a persistent table/container for .graph as well.
        self.graph = {}
        self._node = self.node_dict_factory()
        self._adj = self.adjlist_outer_dict_factory()
        self._pred = predecessors_factory_factory(
            self.sqlitegraph, immutable=immutable
        )()
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

    def iter_edges(self):
        """Roughly equivalent to the .edges interface, but much faster.

        :returns: generator of (u, v, d) similar to .edges, but where d is a
                  dictionary, not an Edge that syncs to database.
        :rtype: tuple generator

        """
        # FIXME: this is currently a read-only strategy (data is converted to dict).
        # We should offer a read-only and non-read-only version downstream.
        return self.sqlitegraph.iter_edges()

    def _create(self):
        self._check_immutable()
        self.sqlitegraph._create_graph()

    def _check_immutable(self):
        if self.immutable:
            raise ImmutableGraphError("Attempted to modify read-only/immutable graph.")
