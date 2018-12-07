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

# FIXME: conversion to built-in networkx graph is way, way too slow (~22 seconds for
# Bellingham). The issue arises from non-batched queries and potentially redundant edges.
# Might be able to exploit cache?

BATCH_SIZE = 500


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
            col_type = sqlite_type(feature["properties"][key])
            cursor.execute("ALTER TABLE nodes ADD COLUMN {} {}".format(key, col_type))
            conn.commit()
            sql_set.append("{}={}".format(k, v))

        keys.append(k)
        values.append(v)

    keys = ["_key"] + [str(k) for k in keys]
    values = [str(key)] + [str(v) for v in values]

    template = "INSERT INTO nodes ({}) VALUES ({})"
    col_str = ", ".join(keys)
    val_str = ", ".join(["?" for v in values])
    sql = template.format(col_str, val_str)
    cursor.execute(sql, values)
    conn.commit()


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
    conn.commit()


def get_edge_attr(conn, u, v):
    ignore_cols = ["_u", "_v"]
    # TODO: some input checking on `key`?
    cursor = conn.cursor()
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]
    query = cursor.execute("SELECT * FROM edges WHERE _u = ? AND _v = ?", (u, v))
    data = {k: v for row in query for k, v in zip(columns, row) if v is not None and
            k not in ignore_cols}
    return data


def add_edge(conn, _u, _v, value, reverse=False):
    if reverse:
        _u, _v = _v, _u

    cursor = conn.cursor()

    # TODO: this is where multi/non-multi graphs would have divergent behavior
    query = cursor.execute("SELECT * FROM edges WHERE _u = ? AND _v = ?", (_u, _v))
    try:
        next(query)
        # Edge already exists - update
        update_edge(conn, _u, _v, value, reverse=reverse)
        return
    except:
        # FIXME: catch iteration error instead
        pass

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
    values = [str(_u), str(_v)] + [str(val) for val in values]

    template = "INSERT INTO edges ({}) VALUES ({})"
    cols_str = ", ".join(keys)
    values_str = ", ".join(["?" for v in values])
    template = template.format(cols_str, values_str)
    cursor.execute(template, values)
    conn.commit()


def update_edge(conn, u, v, value, reverse=False):
    if reverse:
        u, v = v, u
    cursor = conn.cursor()
    columns = [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]

    if value:
        keys = []
        values = []
        for k, val in value.items():
            if k not in columns:
                col_type = sqlite_type(val)
                cursor.execute("ALTER TABLE edges ADD COLUMN {} {}".format(k, col_type))

            keys.append(k)
            values.append(val)

        template = "UPDATE edges SET {} WHERE _u = ? AND _v = ?"
        assignments = ["{} = ?".format(k) for k in keys]
        sql = template.format(", ".join(assignments))
        values.append(u)
        values.append(v)
        cursor.execute(sql, values)
        conn.commit()


# node_dict_factory_factory: creates node_dict_factories that know about the db
# connection
# TODO: use Mapping (mutable?) abstract base class for dict-like magic
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

    def __iter__(self):
        cursor = self.conn.cursor()
        query = cursor.execute("SELECT _key FROM nodes")
        return (row[0] for row in query)

    def __len__(self):
        cursor = self.conn.cursor()
        query = cursor.execute("SELECT count(*) FROM nodes")
        return query.fetchone()[0]


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


# FIXME: inherit from MutableMapping abc, might fix various dict compatibility issues
class EdgeAttr:
    """Retrieves edge attributes from table, allows direct assignment of values as a
    dict-like.

    """

    ignore_cols = ["_u", "_v"]

    def __init__(self, conn=None, u=None, v=None, reverse=False):
        self.conn = conn
        self.reverse = reverse
        if reverse:
            u, v = v, u
        self.u = u
        self.v = v
        self.delayed_attr = {}

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def keys(self):
        if self.reverse:
            return get_edge_attr(self.conn, self.v, self.u).keys()
        else:
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
        self.update({ key: value })

    def __bool__(self):
        if get_edge_attr(self.conn, self.u, self.v):
            return True
        else:
            return False

    def __iter__(self):
        return iter(get_edge_attr(self.conn, self.u, self.v))


def edge_attr_dict_factory_factory(conn):
    def edge_attr_dict_factory():
        edge_data = EdgeAttr(conn)
        return edge_data

    return edge_attr_dict_factory


# adjlist_inner_dict_factory:
# TODO: use Mapping abc for better dict compatibility
class InnerAdjlist:
    ignore_cols = ["_u", "_v"]

    def __init__(self, conn=None, u=None, reverse=False):
        self.conn = conn
        self.u = u
        self.reverse = reverse

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def items(self):
        cursor = self.conn.cursor()
        if self.reverse:
            node_col = "_u"
            where_col = "_v"
        else:
            node_col = "_v"
            where_col = "_u"

        columns = ", ".join([node_col] + self._columns())
        template = "SELECT {}, {} FROM edges WHERE {} = ?".format(node_col, where_col, where_col)
        query = cursor.execute(template, (self.u,))

        return ((row[0], EdgeAttr(self.conn, row[0], row[1], self.reverse)) for row in query)

    def _columns(self):
        # TODO: memoize and/or store as attr, not method
        # TODO: use ignore_cols here?
        cursor = self.conn.cursor()
        columns = []
        for c in cursor.execute("PRAGMA table_info(edges)"):
            if c[1] not in self.ignore_cols:
                columns.append(c[1])
        return columns


    def __getitem__(self, key):
        cursor = self.conn.cursor()
        if self.reverse:
            query = cursor.execute("SELECT * FROM edges WHERE _u = ? AND _v = ?", (key, self.u))
        else:
            query = cursor.execute("SELECT * FROM edges WHERE _u = ? AND _v = ?", (self.u, key))

        if query.fetchone() is not None:
            return EdgeAttr(self.conn, u=self.u, v=key, reverse=self.reverse)
        else:
            raise KeyError("No key {}".format(key))

    def __setitem__(self, key, value):
        cursor = self.conn.cursor()
        columns = self._columns()
        if key in self:
            update_edge(self.conn, key, self.u, value, reverse = self.reverse)
        else:
            add_edge(self.conn, self.u, key, value, reverse = self.reverse)

    def __contains__(self, key):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM edges WHERE _u = ?", (key,))
        try:
            next(cursor)
        except:
            return False
        return True

    def __iter__(self):
        cursor = self.conn.cursor()
        if self.reverse:
            query = cursor.execute("SELECT _u FROM edges WHERE _v = ?", (self.u,))
        else:
            query = cursor.execute("SELECT _v FROM edges WHERE _u = ?", (self.u,))
        return (row[0] for row in query)

    def __len__(self):
        cursor = self.conn.cursor()
        if self.reverse:
            query = cursor.execute("SELECT count(*) FROM edges WHERE _v = ?", (self.u,))
        else:
            query = cursor.execute("SELECT count(*) FROM edges WHERE _u = ?", (self.u,))
        return query.fetchone()[0]


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

    def __init__(self, reverse=False):
        self.reverse = reverse
        if reverse:
            self.key = "_v"
        else:
            self.key = "_u"

    def items(self):
        cursor = self.conn.cursor()
        if self.reverse:
            cols = "_u, _v"
        else:
            cols = "_v, _u"

        query = cursor.execute("SELECT {} FROM edges".format(cols))

        return ((row[0], InnerAdjlist(self.conn, u=row[0], reverse=self.reverse)) for row in query)


    def _columns(self):
        # TODO: memoize and/or store as attr, not method
        # TODO: use ignore_cols here?
        cursor = self.conn.cursor()
        return [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]

    def __getitem__(self, key):
        # Return an atlas view - an inner adjlist
        # TODO: see if it is practical to use conn and _u in Atlas' __init__
        inner = InnerAdjlist()
        inner.conn = self.conn
        inner.u = key
        return inner

    def __contains__(self, key):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM edges WHERE {} = ?".format(self.key), (key,))
        if cursor.fetchone() is not None:
            return True
        return False

    def __setitem__(self, key, value):
        """When assigning inner dict-likes to the outer, we want to convert to a
        'successor' version depending on what type of outer dict-like
        we have.

        """
        if self.reverse:
            value.reverse = True

    def __iter__(self):
        cursor = self.conn.cursor()
        if self.reverse:
            col = "_v"
        else:
            col = "_u"

        query = cursor.execute("SELECT {} FROM EDGES".format(col))
        return (row[0] for row in query)


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


class DiGraphDB(nx.DiGraph):
    def __init__(self, database=None, create=False, *args, **kwargs):
        if database is None:
            database = tempfile.mkstemp()
        self.database = database
        self.conn = self._get_connection()
        if create:
            self._create()

        # The factories of nx dict-likes need to be informed of the connection
        self.node_dict_factory = node_dict_factory_factory(self.conn)
        self.adjlist_outer_dict_factory = adjlist_outer_dict_factory_factory(self.conn)
        self.adjlist_inner_dict_factory = adjlist_inner_dict_factory_factory(self.conn)
        self.edge_attr_dict_factory = dict

        super().__init__(*args, **kwargs)


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
                    raise ValueError("Edge must be 2-tuple of (u, v) or 3-tuple of (u, v, d)")

                _u, _v, d = edge
                keys = []
                values = []
                placeholders = []
                for k, v in d.items():
                    placeholder = "?"
                    if k not in columns:
                        col_type = sqlite_type(v)
                        cursor.execute("ALTER TABLE edges ADD COLUMN {} {}".format(k, col_type))
                        self.conn.commit()

                    if k == "_geometry":
                        placeholder = "GeomFromText(?, 4326)"

                    columns.append(k)
                    keys.append(k)
                    values.append(v)
                    placeholders.append(placeholder)

                query = cursor.execute("SELECT * FROM edges WHERE _u = ? AND _v = ?", (_u, _v))
                if (query.fetchone() is not None) or ((_u, _v) in seen):
                    updates.append(edge)
                else:
                    inserts.append((["_u", "_v"] + keys, [_u, _v] + values, ["?", "?"] + placeholders))
                seen.add((_u, _v))
                nodes.add(_u)
                nodes.add(_v)

            insert_sql = "INSERT INTO edges ({}) VALUES ({})"
            for qkeys, qvalues, qplaceholders in inserts:
                keysub = ", ".join(qkeys)
                placeholdersub = ", ".join(qplaceholders)
                template = insert_sql.format(keysub, placeholdersub)
                cursor.execute(template, qvalues)

            # update_sql = "UPDATE edges SET {} WHERE _u = ? AND _v = ?"
            # for __u, __v, attr in updates:
            #     assignments = ", ".join(["{}=?".format(k) for k in attr.keys()])
            #     template = update_sql.format(assignments)
            #     cursor.execute(template, list(attr.keys()) + [__u, __v])

            cursor.executemany("INSERT OR IGNORE INTO nodes (_key) VALUES (?)", [[n] for n in nodes])

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
        # TODO: investigate when 'AddGeometryColumn' should be added. Might result in
        # speedups?
        has_spatial = cursor.execute("PRAGMA table_info('spatial_ref_sys')")
        try:
            next(has_spatial)
        except StopIteration:
            cursor.execute("SELECT InitSpatialMetaData(1)")
        cursor.executescript("""
            DROP TABLE IF EXISTS edges;
            DROP TABLE IF EXISTS nodes;
            CREATE TABLE edges (_u integer, _v integer, UNIQUE(_u, _v));
            CREATE TABLE nodes (_key, _geometry text, UNIQUE(_key));
            SELECT AddGeometryColumn('edges', '_geometry', 4326, 'LINESTRING')
        """)
        self.conn.commit()

    def _get_connection(self):
        conn = sqlite3.connect(self.database)
        conn.load_extension("mod_spatialite.so")
        return conn
