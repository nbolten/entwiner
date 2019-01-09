"""Dict-like interface(s) for graphs."""
import sqlite3
import tempfile

import networkx as nx

from .utils import sqlite_type

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

BATCH_SIZE = 500
SQL_PLACEHOLDER = "?"
GEOM_SQL_PLACEHOLDER = "GeomFromText(?, 4326)"


class NodeNotFoundError(ValueError):
    pass


class EdgeNotFoundError(ValueError):
    pass


class MissingEdgeError(Exception):
    pass


# FIXME: create an object that knows about the connection and can do stuff like
# add/remove/find edges, use as adapter for other classes.


def add_cols_if_not_exist_sql(conn, colnames, values, table):
    existing_cols = set(
        [c[1] for c in conn.execute("PRAGMA table_info({})".format(table))]
    )
    sql_list = []
    for colname, value in zip(colnames, values):
        if colname not in existing_cols:
            col_type = sqlite_type(ddict[key])
            sql_list.append(
                "ALTER TABLE {} ADD COLUMN {} {};".format(table, colname, col_type)
            )

    return "\n".join(sql_list)


def add_cols_if_not_exist(conn, keys, values, table):
    sql = add_cols_if_not_exist_sql(conn, keys, values, table)
    conn.execute(sql)
    conn.commit()


def get_node(conn, key):
    cursor = conn.cursor()
    query = cursor.execute("SELECT * FROM nodes WHERE _key = ?", (key,))
    data = dict(query.fetchone())
    if data is None:
        raise NodeNotFoundError("Specified node does not exist.")
    data.pop("_key")
    return data


def add_node(conn, key, ddict=None):
    if ddict is None:
        ddict = {}

    cursor = conn.cursor()

    keys, values = zip(*ddict.items())
    add_cols_if_not_exist(conn, keys, values, "nodes")

    template = "INSERT INTO nodes ({}) VALUES ({})"
    col_str = ", ".join(["_key"] + keys)
    val_str = ", ".join(["?" for v in values])
    sql = template.format(col_str, val_str)
    cursor.execute(sql, [str(key)] + values)
    conn.commit()


def update_node(conn, key, ddict):
    if not ddict:
        return
    cursor = conn.cursor()

    keys, values = zip(*ddict.items())
    add_cols_if_not_exist(conn, keys, values, "nodes")

    template = "UPDATE nodes SET {} WHERE _key = ?"
    assignments = ["{} = ?".format(k) for k in keys]
    sql = template.format(", ".join(assignments))
    cursor.execute(sql, (values + [key],))
    conn.commit()


def get_edge_attr(conn, u, v):
    cursor = conn.cursor()
    query = cursor.execute("SELECT * FROM edges WHERE _u = ? AND _v = ?", (u, v))
    row = query.fetchone()
    if row is None:
        raise EdgeNotFoundError("No such edge exists.")

    data = dict(row)
    data.pop("_u")
    data.pop("_v")
    return data


def add_edge_sql(conn, _u, _v, ddict):
    keys, values = zip(*ddict.items())
    add_cols_sql = add_cols_if_not_exist_sql(conn, keys, values, "edges")

    template = "INSERT OR REPLACE INTO edges ({}) VALUES ({});"
    cols_str = ", ".join(["_u", "_v"] + keys)
    values_str = ", ".join(["?" for v in range(len([_u, _v]) + len(values))])
    template = template.format(cols_str, values_str)
    sql = add_cols_sql + "\n" + template
    return sql


def add_edge(conn, _u, _v, ddict):
    sql = add_edge_sql(conn, _u, _v, ddict)
    conn.executescript(sql)
    conn.commit()


def update_edge(conn, u, v, ddict):
    if ddict:
        keys, values = zip(*ddict.items())
        col_sql = add_cols_if_not_exist_sql(conn, keys, values, "edges")

        template = "UPDATE edges SET {} WHERE _u = ? AND _v = ?;"
        assignments = ["{} = ?".format(k) for k in keys]
        sql = template.format(", ".join(assignments))

        conn.execute(col_sql)
        conn.execute(sql, list(values) + [u, v])
        conn.commit()


"""Node class and factory."""
# TODO: use Mapping (mutable?) abstract base class for dict-like magic
class Node:
    def __init__(self, _conn=None, *args, **kwargs):
        self.conn = _conn

    def __getitem__(self, key):
        return get_node(self.conn, key)

    def __contains__(self, key):
        try:
            self[key]
        except NodeNotFoundError:
            return False
        return True

    def __setitem__(self, key, ddict):
        if key in self:
            update_node(self.conn, key, ddict)
        else:
            add_node(self.conn, key, ddict)

    def __iter__(self):
        query = self.conn.execute("SELECT _key FROM nodes")
        return (row[0] for row in query)

    def __len__(self):
        query = self.conn.execute("SELECT count(*) FROM nodes")
        return query.fetchone()[0]


def node_factory_factory(conn):
    """Creates factories of DB-based Nodes.

    :param conn: An SQLite database connection.
    :type conn: sqlite3.Connection
    :returns: callable that creates a dict-like nodes interface (keys = node ID,
              values = node attributes).

    """

    def node_factory():
        return Node(_conn=conn)

    return node_factory


"""Edge class + factory."""
# FIXME: inherit from MutableMapping abc, might fix various dict compatibility issues
class Edge:
    """Retrieves edge attributes from table, allows direct assignment of values as a
    dict-like.

    """

    def __init__(self, _conn=None, _u=None, _v=None):
        self.conn = _conn
        self.u = _u
        self.v = _v
        self.delayed_attr = {}

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def keys(self):
        return get_edge_attr(self.conn, self.u, self.v).keys()

    def items(self):
        return get_edge_attr(self.conn, self.u, self.v).items()

    def update(self, attr):
        if self.u is not None and self.v is not None:
            update_edge(self.conn, self.u, self.v, attr)
        else:
            self.delayed_attr.update(attr)

    def commit_attrs(self):
        if self.delayed_attr:
            if self.u is not None and self.v is not None:
                update_edge(self.conn, self.u, self.v, self.delayed_attr)
            else:
                # TODO: make into separate error class
                raise Exception("Can't commit attrs to edge without u and v.")

    def __getitem__(self, key):
        return get_edge_attr(self.conn, self.u, self.v)[key]

    def __setitem__(self, key, value):
        self.update({key: value})

    def __bool__(self):
        if get_edge_attr(self.conn, self.u, self.v):
            return True
        else:
            return False

    def __iter__(self):
        return iter(get_edge_attr(self.conn, self.u, self.v))


def edge_factory_factory(conn):
    def edge_factory():
        return Edge(conn)

    return edge_factory


"""Outer adjacency list classes + factories."""


class Successors:
    def __init__(self, _conn=None):
        self.conn = _conn

    def items(self):
        query = self.conn.execute("SELECT _u FROM edges")
        return ((row[0], InnerAdjlist(self.conn, row[0], False)) for row in query)

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        return InnerAdjlist(self.conn, key, False)

    def __contains__(self, key):
        query = self.conn.execute("SELECT * FROM edges WHERE _u = ?", (key,))
        if query.fetchone() is not None:
            return True
        return False

    def __iter__(self):
        query = self.conn.execute("SELECT DISTINCT _u FROM edges")
        return (row[0] for row in query)

    def __setitem__(self, key, ddict):
        # Plan to drop any pre-existing edges using this key.
        del_sql = "DELETE FROM edges WHERE _u = ?"
        self.conn.execute(del_sql, (key,))
        # Create the InnerAdjlist and edges representing the data
        inserts = []
        for neighbor, edge_data in ddict.items():
            inserts.append(add_edge_sql(key, neighbor, edge_data))
        self.conn.executescript("\n".join(inserts))


class Predecessors:
    def __init__(self, _conn=None):
        self.conn = _conn

    def items(self):
        query = self.conn.execute("SELECT _v FROM edges")
        return ((row[0], InnerAdjlist(self.conn, row[0], True)) for row in query)

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        return InnerAdjlist(self.conn, key, True)

    def __contains__(self, key):
        query = self.conn.execute("SELECT * FROM edges WHERE _v = ?", (key,))
        if query.fetchone() is not None:
            return True
        return False

    def __iter__(self):
        query = self.conn.execute("SELECT DISTINCT _v FROM edges")
        return (row[0] for row in query)

    def __setitem__(self, key, ddict):
        # Plan to drop any pre-existing edges using this key.
        del_sql = "DELETE FROM edges WHERE _v = ?;"
        self.conn.execute(del_sql, (key,))
        # Create the InnerAdjlist and edges representing the data
        inserts = []
        for neighbor, edge_data in ddict.items():
            inserts.append(add_edge_sql(neighbor, key, edge_data))
        self.conn.executescript("\n".join(inserts))


def predecessors_factory_factory(conn):
    def predecessors_factory():
        return Predecessors(_conn=conn)

    return predecessors_factory


def successors_factory_factory(conn):
    def successors_factory():
        return Successors(_conn=conn)

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

    def __init__(self, _conn=None, _key=None, _pred=False):
        self.conn = _conn
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
            query = self.conn.execute(
                "SELECT _v, _u FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.conn.execute(
                "SELECT _u, _v FROM edges WHERE _u = ?", (self.key,)
            )
        return ((row[1], Edge(self.conn, row[0], row[1])) for row in query)

    def __getitem__(self, key):
        if self.pred:
            query = self.conn.execute(
                "SELECT * FROM edges WHERE _u = ? AND _v = ?", (key, self.key)
            )
        else:
            query = self.conn.execute(
                "SELECT * FROM edges WHERE _u = ? AND _v = ?", (self.key, key)
            )

        if query.fetchone() is not None:
            return Edge(self.conn, _u=self.key, _v=key)
        else:
            raise KeyError("No key {}".format(key))

    def __setitem__(self, key, value):
        if key in self:
            if self.pred:
                update_edge(self.conn, key, self.key, value)
            else:
                update_edge(self.conn, self.key, key, value)
        else:
            if self.pred:
                add_edge(self.conn, key, self.key, value)
            else:
                add_edge(self.conn, self.key, key, value)

    def __contains__(self, key):
        if self.pred:
            query = self.conn.execute("SELECT * FROM edges WHERE _v = ?", (key,))
        else:
            query = self.conn.execute("SELECT * FROM edges WHERE _u = ?", (key,))
        try:
            next(query)
        except:
            return False
        return True

    def __iter__(self):
        if self.pred:
            query = self.conn.execute("SELECT _u FROM edges WHERE _v = ?", (self.key,))
        else:
            query = self.conn.execute("SELECT _v FROM edges WHERE _u = ?", (self.key,))
        return (row[0] for row in query)

    def __len__(self):
        if self.pred:
            query = self.conn.execute(
                "SELECT count(*) FROM edges WHERE _v = ?", (self.key,)
            )
        else:
            query = self.conn.execute(
                "SELECT count(*) FROM edges WHERE _u = ?", (self.key,)
            )
        return query.fetchone()[0]


def adjlist_inner_dict_factory_factory(conn):
    def adjlist_inner_dict_factory():
        return InnerAdjlist(conn)

    return adjlist_inner_dict_factory


# class AtlasView:
#     def __init__(self, _conn, _u):
#         self.conn = _conn
#         self.u = _u
#
#     def get(self, key, default):
#         try:
#             return self[key]
#         except KeyError:
#             return default
#
#     def __getitem__(self, key):
#         try:
#             return Edge(self.conn, self.u, key)
#         except MissingEdgeError:
#             return KeyError
#
#     # NOTE: __setitem__ is left undefined on purpose to match NetworkX 2.0 behavior


class DiGraphDB(nx.DiGraph):
    def __init__(self, incoming_graph_data=None, database=None, create=False, **attr):
        if database is None:
            database = tempfile.mkstemp()
        self.database = database
        self.conn = self._get_connection()
        if create:
            self._create()

        # The factories of nx dict-likes need to be informed of the connection
        self.node_dict_factory = node_factory_factory(self.conn)
        self.adjlist_outer_dict_factory = successors_factory_factory(self.conn)
        self.adjlist_inner_dict_factory = adjlist_inner_dict_factory_factory(self.conn)
        self.edge_attr_dict_factory = dict

        self.graph = {}
        self._node = self.node_dict_factory()
        self._adj = self.adjlist_outer_dict_factory()
        self._pred = predecessors_factory_factory(self.conn)()
        self._succ = self._adj

        if incoming_graph_data is not None:
            nx.convert.to_networkx_graph(incoming_graph_data, create_using=self)
        self.graph.update(attr)

    def add_edges_from(self, ebunch_to_add, _batch_size=BATCH_SIZE, **attr):
        if _batch_size < 2:
            # User has entered invalid number (negative, zero) or 1. Use default behavior.
            super().add_edges_from(self, ebunch_to_add, **attr)
            return

        # Add multiple edges at once - saves time (1000X+ faster) on inserts
        cursor = self.conn.cursor()

        def add_edges(ebunch, **attr):
            # Inserting one at a time is slow, so do it in a batch - need to iterate over
            # the ebunch once to check for new columns, then insert multiple at a time
            columns = [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]
            inserts = []
            updates = []

            nodes = set([])
            seen = set([])
            for edge in ebunch:
                if len(edge) == 2:
                    edge = (edge[0], edge[1], attr)
                elif len(edge) == 3:
                    edge = (edge[0], edge[1], {**attr, **edge[2]})
                else:
                    raise ValueError(
                        "Edge must be 2-tuple of (u, v) or 3-tuple of (u, v, d)"
                    )

                _u, _v, d = edge
                keys = []
                values = []
                placeholders = []
                for k, v in d.items():
                    if k not in columns:
                        col_type = sqlite_type(v)
                        cursor.execute(
                            "ALTER TABLE edges ADD COLUMN {} {}".format(k, col_type)
                        )
                        self.conn.commit()

                    if k == "_geometry":
                        placeholders.append(GEOM_SQL_PLACEHOLDER)
                    else:
                        placeholders.append(SQL_PLACEHOLDER)

                    columns.append(k)
                    keys.append(k)
                    values.append(v)

                query = cursor.execute(
                    "SELECT * FROM edges WHERE _u = ? AND _v = ?", (_u, _v)
                )
                if (query.fetchone() is not None) or ((_u, _v) in seen):
                    updates.append(edge)
                else:
                    inserts.append(
                        (
                            ["_u", "_v"] + keys,
                            [_u, _v] + values,
                            ["?", "?"] + placeholders,
                        )
                    )
                seen.add((_u, _v))
                nodes.add(_u)
                nodes.add(_v)

            insert_sql = "INSERT INTO edges ({}) VALUES ({})"
            for qkeys, qvalues, qplaceholders in inserts:
                keysub = ", ".join(qkeys)
                placeholdersub = ", ".join(qplaceholders)
                template = insert_sql.format(keysub, placeholdersub)
                cursor.execute(template, qvalues)

            cursor.executemany(
                "INSERT OR IGNORE INTO nodes (_key) VALUES (?)", [[n] for n in nodes]
            )

            self.conn.commit()

        ebunch_iter = iter(ebunch_to_add)
        ebunch = []
        while True:
            try:
                edge = next(ebunch_to_add)
                ebunch.append(edge)
            except StopIteration as e:
                add_edges(ebunch, **attr)
                break

            if len(ebunch) == _batch_size:
                add_edges(ebunch, **attr)
                ebunch = []

    def _create(self):
        # Create the tables
        cursor = self.conn.cursor()
        has_spatial = cursor.execute("PRAGMA table_info('spatial_ref_sys')")
        try:
            next(has_spatial)
        except StopIteration:
            cursor.execute("SELECT InitSpatialMetaData(1)")
        cursor.execute("DROP TABLE IF EXISTS edges")
        cursor.execute("DROP TABLE IF EXISTS nodes")
        cursor.execute("CREATE TABLE nodes (_key, _geometry text, UNIQUE(_key))")
        cursor.execute("CREATE TABLE edges (_u integer, _v integer, UNIQUE(_u, _v))")
        q = cursor.execute(
            "SELECT * FROM geometry_columns WHERE f_table_name = 'edges' AND f_geometry_column = '_geometry'"
        )
        if q.fetchone() is None:
            cursor.execute(
                "SELECT AddGeometryColumn('edges', '_geometry', 4326, 'LINESTRING')"
            )
        self.conn.commit()

    def _get_connection(self):
        conn = sqlite3.connect(self.database)
        conn.row_factory = sqlite3.Row
        conn.load_extension("mod_spatialite.so")
        return conn
