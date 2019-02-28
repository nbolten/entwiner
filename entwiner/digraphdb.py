"""Dict-like interface(s) for graphs."""
import sqlite3
import tempfile

import networkx as nx

from .utils import sqlite_type
from .graphdb import GraphDB
from .edges import Edge, RealizedEdge
from .nodes import Node

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

BATCH_SIZE = 10000
PLACEHOLDER = "?"
GEOM_PLACEHOLDER = "GeomFromText(?, 4326)"


def node_factory_factory(graphdb):
    """Creates factories of DB-based Nodes.
    """

    def node_factory():
        return Node(_graphdb=graphdb)

    return node_factory


def edge_factory_factory(conn):
    def edge_factory():
        return Edge(conn)

    return edge_factory


"""Outer adjacency list classes + factories."""


class Successors:
    def __init__(self, _graphdb=None):
        self.graphdb = _graphdb

    def clear(self):
        # What should this do? Is it safe to drop all rows given that predecessors
        # might still be defined?
        pass

    def items(self):
        query = self.graphdb.conn.execute("SELECT _u FROM edges")
        return ((row[0], InnerAdjlist(self.graphdb, row[0], False)) for row in query)

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        return InnerAdjlist(self.graphdb, key, False)

    def __contains__(self, key):
        query = self.graphdb.conn.execute("SELECT * FROM edges WHERE _u = ?", (key,))
        if query.fetchone() is not None:
            return True
        return False

    def __iter__(self):
        query = self.graphdb.conn.execute("SELECT DISTINCT _u FROM edges")
        return (row[0] for row in query)

    def __setitem__(self, key, ddict):
        # Plan to drop any pre-existing edges using this key.
        del_sql = "DELETE FROM edges WHERE _u = ?"
        self.graphdb.conn.execute(del_sql, (key,))
        # Create the InnerAdjlist and edges representing the data
        inserts = []
        for neighbor, edge_data in ddict.items():
            inserts.append(add_edge_sql(key, neighbor, edge_data))
        self.graphdb.conn.executescript("\n".join(inserts))


class Predecessors:
    def __init__(self, _graphdb=None):
        self.graphdb = _graphdb

    def clear(self):
        pass

    def items(self):
        query = self.graphdb.conn.execute("SELECT _v FROM edges")
        return ((row[0], InnerAdjlist(self.graphdb, row[0], True)) for row in query)

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        return InnerAdjlist(self.graphdb, key, True)

    def __contains__(self, key):
        query = self.graphdb.conn.execute("SELECT * FROM edges WHERE _v = ?", (key,))
        if query.fetchone() is not None:
            return True
        return False

    def __iter__(self):
        query = self.graphdb.conn.execute("SELECT DISTINCT _v FROM edges")
        return (row[0] for row in query)

    def __setitem__(self, key, ddict):
        # Plan to drop any pre-existing edges using this key.
        del_sql = "DELETE FROM edges WHERE _v = ?;"
        self.graphdb.conn.execute(del_sql, (key,))
        # Create the InnerAdjlist and edges representing the data
        inserts = []
        for neighbor, edge_data in ddict.items():
            inserts.append(add_edge_sql(neighbor, key, edge_data))
        self.graphdb.conn.executescript("\n".join(inserts))


def predecessors_factory_factory(graphdb):
    def predecessors_factory():
        return Predecessors(_graphdb=graphdb)

    return predecessors_factory


def successors_factory_factory(graphdb):
    def successors_factory():
        return Successors(_graphdb=graphdb)

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

    def __init__(self, _graphdb=None, _key=None, _pred=False):
        self.graphdb = _graphdb
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
        # TODO: point for optimization: make queries into constants.
        if self.pred:
            query = self.graphdb.conn.execute(
                "SELECT _v, _u FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.graphdb.conn.execute(
                "SELECT _u, _v FROM edges WHERE _u = ?", (self.key,)
            )
        return ((row[1], Edge(self.graphdb, row[0], row[1])) for row in query)

    def __getitem__(self, key):
        if self.pred:
            query = self.graphdb.conn.execute(
                "SELECT * FROM edges WHERE _u = ? AND _v = ?", (key, self.key)
            )
        else:
            query = self.graphdb.conn.execute(
                "SELECT * FROM edges WHERE _u = ? AND _v = ?", (self.key, key)
            )

        if query.fetchone() is not None:
            return Edge(self.graphdb, _u=self.key, _v=key)
        else:
            raise KeyError("No key {}".format(key))

    def __setitem__(self, key, value):
        if key in self:
            if self.pred:
                self.graphdb.update_edge(key, self.key, value)
            else:
                self.graphdb.update_edge(self.key, key, value)
        else:
            if self.pred:
                self.graphdb.add_edge(key, self.key, value)
            else:
                self.graphdb.add_edge(self.key, key, value)

    def __contains__(self, key):
        if self.pred:
            query = self.graphdb.conn.execute(
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
            query = self.graphdb.conn.execute(
                "SELECT _u FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.graphdb.conn.execute(
                "SELECT _v FROM edges WHERE _u = ?", (self.key,)
            )
        return (row[0] for row in query)

    def __len__(self):
        if self.pred:
            query = self.graphdb.conn.execute(
                "SELECT count(*) FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.graphdb.conn.execute(
                "SELECT count(*) FROM edges WHERE _u = ?", (self.key,)
            )
        return query.fetchone()[0]


def adjlist_inner_factory_factory(graphdb):
    def adjlist_inner_factory():
        return InnerAdjlist(graphdb)

    return adjlist_inner_factory


class DiGraphDB(nx.DiGraph):
    def __init__(
        self, incoming_graph_data=None, path=None, graphdb=None, create=False, **attr
    ):
        if graphdb is None:
            if path is None:
                n, path = tempfile.mkstemp()
            graphdb = GraphDB(path)
        self.graphdb = graphdb
        if create:
            self._create()

        # The factories of nx dict-likes need to be informed of the connection
        self.node_dict_factory = node_factory_factory(self.graphdb)
        self.adjlist_outer_dict_factory = successors_factory_factory(self.graphdb)
        self.adjlist_inner_dict_factory = adjlist_inner_factory_factory(self.graphdb)
        self.edge_attr_dict_factory = dict

        # FIXME: should use a persistent table/container for .graph as well.
        self.graph = {}
        self._node = self.node_dict_factory()
        self._adj = self.adjlist_outer_dict_factory()
        self._pred = predecessors_factory_factory(self.graphdb)()
        self._succ = self._adj

        if incoming_graph_data is not None:
            nx.convert.to_networkx_graph(incoming_graph_data, create_using=self)
        self.graph.update(attr)

    def add_edges_from(self, ebunch_to_add, _batch_size=BATCH_SIZE, **attr):
        """We had to override this method in order to add the batch_size param.

        """
        if _batch_size < 2:
            # User has entered invalid number (negative, zero) or 1. Use default behavior.
            super().add_edges_from(self, ebunch_to_add, **attr)
            return

        # Add multiple edges at once - saves time (1000X+ faster) on inserts
        conn = self.graphdb.conn

        def add_edges(ebunch, **attr):
            # Inserting one at a time is slow, so do it in a batch - need to iterate over
            # the ebunch once to check for new columns, then insert multiple at a time
            def prepare_data(ebunch, attr):
                edge_columns = [c[1] for c in conn.execute("PRAGMA table_info(edges)")]

                edges_values = []
                nodes_values = []
                seen = set([])
                for edge in ebunch:
                    if len(edge) == 2:
                        _u = edge[0]
                        _v = edge[1]
                        d = attr
                    elif len(edge) == 3:
                        _u = edge[0]
                        _v = edge[1]
                        d = {**attr, **edge[2]}
                    else:
                        # TODO: this doesn't seem useful. Skip + warn / raise other
                        # error?
                        raise ValueError(
                            "Edge must be 2-tuple of (u, v) or 3-tuple of (u, v, d)"
                        )

                    # Check for edge already existing. Skip.
                    # TODO: Issue a warning?
                    query = conn.execute(
                        "SELECT * FROM edges WHERE _u = ? AND _v = ?", (_u, _v)
                    )
                    if (query.fetchone() is not None) or ((_u, _v) in seen):
                        continue

                    # TODO: convert to WKT at this step rather than i/o?
                    values = [_u, _v]
                    # Skip first two columns - these are _u and _v and we already
                    # accounted for them
                    for c in edge_columns[2:]:
                        try:
                            value = d.pop(c)
                        except KeyError:
                            value = None
                        values.append(value)
                    if d:
                        # There are new columns!
                        for key, value in d.items():
                            sqltype = sqlite_type(value)
                            conn.execute(
                                "ALTER TABLE edges ADD COLUMN {} {}".format(
                                    key, sqltype
                                )
                            )
                            conn.commit()
                            edge_columns.append(key)
                            values.append(value)

                    edges_values.append(values)

                    # TODO: might save some time by not doing redundant node creation
                    # code (check if the node already exists)
                    for node in (_u, _v):
                        node_geom = "POINT(" + " ".join(node.split(", ")) + ")"
                        nodes_values.append((node, node_geom))

                    seen.add((_u, _v))

                return edge_columns, edges_values, nodes_values

            edge_columns, edges_values, nodes_values = prepare_data(ebunch, attr)

            placeholders = [
                GEOM_PLACEHOLDER if c == "_geometry" else PLACEHOLDER
                for c in edge_columns
            ]
            edges_template = "INSERT OR IGNORE INTO edges ({}) VALUES ({})"
            edges_sql = edges_template.format(
                ", ".join(edge_columns), ", ".join(placeholders)
            )
            conn.executemany(edges_sql, edges_values)

            conn.executemany(
                "INSERT OR IGNORE INTO nodes (_key, _geometry) VALUES (?, GeomFromText(?, 4326))",
                nodes_values,
            )
            conn.commit()

        ebunch_iter = iter(ebunch_to_add)
        ebunch = []
        while True:
            try:
                edge = next(ebunch_to_add)
                ebunch.append(edge)
            except StopIteration:
                add_edges(ebunch, **attr)
                break

            if len(ebunch) == _batch_size:
                add_edges(ebunch, **attr)
                ebunch = []

    def edges_iter(self):
        """Roughly equivalent to the .edges interface, but much faster."""
        conn = self.graphdb.conn
        query = conn.execute("SELECT * FROM edges")
        for row in query:
            row = dict(row)
            u = row.pop("_u")
            v = row.pop("_v")
            yield (
                u,
                v,
                RealizedEdge(**{k: v for k, v in row.items() if v is not None}),
            )

    def _create(self):
        # Create the tables
        # FIXME: this is specific to sqlite and spatial data - needs to be isolate
        # into separate class, not general-purpose graph class
        conn = self.graphdb.conn
        has_spatial = conn.execute("PRAGMA table_info('spatial_ref_sys')")
        try:
            next(has_spatial)
        except StopIteration:
            conn.execute("SELECT InitSpatialMetaData(1)")
        conn.commit()
        self._create_edge_table()
        self._create_node_table()

    def _create_edge_table(self):
        conn = self.graphdb.conn
        conn.execute("DROP TABLE IF EXISTS edges")
        conn.execute(
            "CREATE TABLE edges (_u integer, _v integer, _layer text, UNIQUE(_u, _v))"
        )
        q = conn.execute(
            "SELECT * FROM geometry_columns WHERE f_table_name = 'edges' AND f_geometry_column = '_geometry'"
        )
        if q.fetchone() is None:
            conn.execute(
                "SELECT AddGeometryColumn('edges', '_geometry', 4326, 'LINESTRING')"
            )
            conn.execute("SELECT CreateSpatialIndex('edges', '_geometry')")
        conn.commit()

    def _create_node_table(self):
        conn = self.graphdb.conn
        conn.execute("DROP TABLE IF EXISTS nodes")
        conn.execute("CREATE TABLE nodes (_key, UNIQUE(_key))")
        q = conn.execute(
            "SELECT * FROM geometry_columns WHERE f_table_name = 'nodes' AND f_geometry_column = '_geometry'"
        )
        if q.fetchone() is None:
            conn.execute(
                "SELECT AddGeometryColumn('nodes', '_geometry', 4326, 'POINT')"
            )
            conn.execute("SELECT CreateSpatialIndex('nodes', '_geometry')")
        conn.commit()
