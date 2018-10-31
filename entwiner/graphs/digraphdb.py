"""Dict-like interface(s) for graphs."""
import sqlite3

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

class MissingEdgeError(Exception):
    pass

def get_node(conn, key):
    ignore_cols = ["_key"]
    # TODO: some input checking on `key`?
    cursor = conn.cursor()
    query = cursor.execute("SELECT * FROM nodes WHERE _key = ?", (key,))
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(nodes)")]
    data = {k: v for row in query for k, v in zip(columns, row) if v is not None and
            k not in ignore_cols}
    return data


def add_node(conn, key, value):
    cursor = conn.cursor()
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(nodes)")]

    keys = []
    values = []
    for k, v in value.items():
        if k not in columns:
            col_type = sqlite_type(feature['properties'][key])
            cursor.execute("ALTER TABLE nodes ADD COLUMN {} {}".format(key, col_type))
            sql_set.append("{}={}".format(k, v))

        keys.append(k)
        values.append(v)

    keys = ["_key"] + [str(k) for k in keys]
    values = [str(key)] + [str(v) for v in values]

    template = "INSERT INTO nodes ({}) VALUES ({})"
    sql = template.format(", ".join(keys), ", ".join(values))
    cursor.execute(sql)


def update_node(conn, key, value):
    if not value:
        return
    cursor = conn.cursor()
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(nodes)")]

    keys = []
    values = []
    for k, v in value.items():
        if k not in columns:
            col_type = sqlite_type(v)
            cursor.execute("ALTER TABLE nodes ADD COLUMN {} {}".format(key, col_type))
            sql_set.append("{}={}".format(k, v))

        keys.append(k)
        values.append(v)

    template = "UPDATE nodes SET {} WHERE _key = ?"
    assignments = ["{} = {}".format(k, v) for k, v in zip(keys, values)]
    sql = template.format(", ".join(assignments))
    cursor.execute(sql, (key,))


def get_edge(conn, u, v):
    ignore_cols = ["_u", "_v"]
    # TODO: some input checking on `key`?
    cursor = conn.cursor()
    query = cursor.execute("SELECT * FROM edges WHERE _u = ? AND _v = ?", (u, v))
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]
    data = {k: v for row in query for k, v in zip(columns, row) if v is not None and
            k not in ignore_cols}
    return data


def add_edge(conn, u, v, value, succ=False):
    if succ:
        u, v = v, u
    cursor = conn.cursor()
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]

    keys = []
    values = []
    for k, v in value.items():
        if k not in columns:
            col_type = sqlite_type(v)
            cursor.execute("ALTER TABLE edges ADD COLUMN {} {}".format(k, col_type))

        keys.append(k)
        values.append(v)

    keys = ["_u", "_v"] + [str(k) for k in keys]
    values = [str(u), str(v)] + [str(val) for val in values]

    template = "INSERT INTO edges ({}) VALUES ({})"
    sql = template.format(", ".join(keys), ", ".join(values))
    cursor.execute(sql)


def update_edge(conn, u, v, value, succ=False):
    if succ:
        u, v = v, u
    cursor = conn.cursor()
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]

    if value:
        keys = []
        values = []
        for k, val in value.items():
            if k not in columns:
                col_type = sqlite_type(v)
                cursor.execute("ALTER TABLE edges ADD COLUMN {} {}".format(k, col_type))

            keys.append(k)
            values.append(val)

        template = "UPDATE edges SET {} WHERE _u = ? AND _v = ?"
        assignments = ["{} = ?".format(k, val) for k, val in zip(keys, values)]
        sql = template.format(", ".join(assignments))
        values.append(u)
        values.append(v)
        print(sql)
        print(values)
        cursor.execute(sql, values)


# node_dict_factory_factory: creates node_dict_factories that know about the db
# connection
class NodeDB:
    ignore_cols = ["_key"]

    def _columns(self):
        # TODO: memoize and/or store as attr, not method
        cursor = self.conn.cursor()
        return [c[1] for c in cursor.execute("PRAGMA table_info(nodes)")]

    def __getitem__(self, key):
        # TODO: some input checking on `key`?
        cursor = self.conn.cursor()
        query = cursor.execute("SELECT * FROM nodes WHERE _key = ?", (key,))
        columns = self._columns()  # TODO: memoize and/or store as attr, not method
        data = {k: v for row in query for k, v in zip(columns, row) if v is not None and
                k not in self.ignore_cols}
        return data

    def __contains__(self, key):
        cursor = self.conn.cursor()
        query = cursor.execute("SELECT * FROM nodes WHERE _key = ?", (key,))
        try:
            next(query)
        except:
            return False

        return True

    def __setitem__(self, key, value):
        # TODO: some input checking on `key` and `value`?
        # value is assumed to be a flat dict-like
        if key in self:
            # If key already exists in db, update values
            update_node(self.conn, key, value)
        else:
            add_node(self.conn, key, value)


def node_dict_factory_factory(conn):
    """Creates node_dict_factories that know about a shared SQLite connection.

    :param conn: An SQLite database connection.
    :type conn: sqlite3.Connection
    :returns: callable that creates a dict-like nodes interface (keys = node ID,
              values = node attributes).

    """
    def node_dict_factory():
        nodes = NodeDB()
        nodes.conn = conn

        return nodes

    return node_dict_factory


class EdgeAttr:
    """Retrieves edge attributes from table, allows direct assignment of values as a
    dict-like.

    """

    ignore_cols = ["_u", "_v"]

    def __init__(self, conn=None, u=None, v=None, succ=False):
        self.conn = conn
        if succ:
            u, v = v, u
        self.u = u
        self.v = v

    def items(self):
        return get_edge(self.conn, self.u, self.v).items()

    def update(self, attr):
        update_edge(self.conn, self.u, self.v, attr)

    def __getitem__(self, key):
        return get_edge(self.conn, self.u, self.v)[key]

    def __setitem__(self, key, value):
        update_edge(self.conn, self.u, self.v, value)

    def __bool__(self):
        if get_edge(self.conn, self.u, self.v):
            return True
        else:
            return False


def edge_attr_dict_factory_factory(conn):
    def edge_attr_dict_factory():
        edge_data = EdgeAttr(conn)
        return edge_data

    return edge_attr_dict_factory


# adjlist_inner_dict_factory:
class InnerAdjlist:
    ignore_cols = ["_u", "_v"]
    succ = False

    def _columns(self):
        # TODO: memoize and/or store as attr, not method
        # TODO: use ignore_cols here?
        cursor = self.conn.cursor()
        return [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def __getitem__(self, key):
        return EdgeAttr(self.conn, u=self.u, v=key, succ=self.succ)

    def __contains__(self, key):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM edges WHERE _u = ?", (key,))
        try:
            next(cursor)
        except:
            return False
        return True

    def __setitem__(self, key, value):
        cursor = self.conn.cursor()
        columns = self._columns()
        if key in self:
            update_edge(self.conn, key, self.u, value, succ=self.succ)
        else:
            add_edge(self.conn, self.u, key, value, succ=self.succ)
        self.conn.commit()


# class InnerSuccAdjlist:
#     ignore_cols = ["_u", "_v"]
#
#     def _columns(self):
#         # TODO: memoize and/or store as attr, not method
#         # TODO: use ignore_cols here?
#         cursor = self.conn.cursor()
#         return [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]
#
#     def get(self, key, defaults):
#         try:
#             return self[key]
#         except KeyError:
#             return defaults
#
#     def __getitem__(self, key):
#         return EdgeAttr(self.conn, u=key, v=self.u)
#
#     def __setitem__(self, key, value):
#         # TODO: some input checking on `key` and `value`?
#         # value is assumed to be a flat dict-like
#         cursor = self.conn.cursor()
#         columns = self._columns()
#         if self[key]:
#             # If key already exists in db, update values
#             update_edge(self.conn, key, self.u, value)
#         else:
#             add_edge(self.conn, key, self.u, value)
#         self.conn.commit()


def adjlist_inner_dict_factory_factory(conn):
    def adjlist_inner_dict_factory():
        adjlist_inner = InnerAdjlist()
        adjlist_inner.conn = conn

        return adjlist_inner

    return adjlist_inner_dict_factory


# adjlist_outer_dict_factory:
class OuterAdjlist:
    """

    This could be used for predecessors or successors in the graph - in other words,
    the 'key' lookup could be for u (pred node ref) or v (end node ref) depending on
    how NetworkX is internally using the class. NetworkX does not have any documented
    way of having distinct adjlist classes, so we will implement our own two classes
    and override __init__ in DiGraph.

    """
    ignore_cols = ["_u", "_v"]

    def __init__(self, succ=False):
        self.succ = succ
        if succ:
            self.key = "_v"
        else:
            self.key = "_u"

    def _columns(self):
        # TODO: memoize and/or store as attr, not method
        # TODO: use ignore_cols here?
        cursor = self.conn.cursor()
        return [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        # TODO: see if it is practical to use conn and _u in Atlas` __init__
        inner = InnerAdjlist()
        inner.conn = self.conn
        inner.u = key
        return inner

    def __contains__(self, key):
        cursor = self.conn.cursor()
        sql = "SELECT * FROM edges WHERE {} = ?".format(self.key)
        query = cursor.execute(sql, (key,))
        try:
            next(query)
        except:
            return False

        return True

    def __setitem__(self, key, value):
        """When assigning inner dict-likes to the outer, we want to convert to a
        'successor' version depending on what type of outer dict-like
        we have.

        """
        if self.succ:
            value.succ = True

def adjlist_outer_dict_factory_factory(conn):
    def adjlist_outer_dict_factory():
        adjlist_outer = OuterAdjlist()
        adjlist_outer.conn = conn

        return adjlist_outer

    return adjlist_outer_dict_factory


def pred_dict_factory_factory(conn):
    def pred_dict_factory():
        adjlist_outer = Pred()
        adjlist_outer.conn = conn

        return adjlist_outer

    return pred_outer_dict_factory


def succ_dict_factory_factory(conn):
    def succ_dict_factory():
        adjlist_outer = Succ()
        adjlist_outer.conn = conn

        return adjlist_outer

    return succ_outer_dict_factory


class AtlasView:
    def __init__(self, conn, u):
        self.conn = conn
        self.u = u

    def get(self, key, default):
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, key):
        try:
            return EdgeAttr(self.conn, self.u, key)
        except MissingEdgeError:
            return KeyError

    # NOTE: __setitem__ is undefined on purpose so that it matches NetworkX 2.0


def digraphdb(database, recreate=False):
    conn = sqlite3.connect(database)
    if recreate:
        # Create the tables
        cursor = conn.cursor()
        cursor.executescript("""
            DROP TABLE IF EXISTS edges;
            DROP TABLE IF EXISTS nodes;
            CREATE TABLE edges (_u integer, _v integer, UNIQUE(_u, _v));
            CREATE TABLE nodes (_key, UNIQUE(_key));
        """)

    def __init__(self, *arg, **kwarg):
        super(*arg, **kwarg)
        self._pred = OuterAdjlist()
        self._succ = OuterAdjlist(succ=True)

    return type('DiGraphDB', (nx.DiGraph, ), {
        "node_dict_factory": staticmethod(node_dict_factory_factory(conn)),
        "adjlist_outer_dict_factory": staticmethod(adjlist_outer_dict_factory_factory(conn)),
        "adjlist_inner_dict_factory": staticmethod(adjlist_inner_dict_factory_factory(conn)),
        "edge_attr_dict_factory": staticmethod(edge_attr_dict_factory_factory(conn)),
    })()
