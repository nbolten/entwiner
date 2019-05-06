import json
import sqlite3

from .utils import sqlite_type
from .exceptions import EdgeNotFound, NodeNotFound

PLACEHOLDER = "?"
GEOM_PLACEHOLDER = "GeomFromText(?, 4326)"

# TODO: generalize to non-sqlite3 DBs
class SQLiteGraph:
    def __init__(self, path):
        self.path = path
        self.conn = self.connect()

    def connect(self):
        def dict_factory(cursor, row):
            # TODO: evaluate performance overhead of doing non-null check here
            return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

        conn = sqlite3.connect(self.path)
        conn.row_factory = dict_factory
        # conn.row_factory = sqlite3.Row
        conn.enable_load_extension(True)
        conn.load_extension("mod_spatialite.so")
        return conn

    def execute(self, sql, values=()):
        return self.conn.execute(sql, values)

    def executemany(self, sql, values):
        return self.conn.executemany(sql, values)

    def commit(self):
        return self.conn.commit()

    def reindex(self):
        edges_index = ", ".join(self.get_edges_columns())
        nodes_index = ", ".join(self.get_nodes_columns())
        sql = [
            "DROP INDEX IF EXISTS edges_covering",
            "CREATE INDEX edges_covering ON edges ({})".format(edges_index),
            "DROP INDEX IF EXISTS nodes_covering",
            "CREATE INDEX nodes_covering ON edges ({})".format(nodes_index),
        ]
        for s in sql:
            self.execute(s)
        self.commit()

    def _create_graph(self):
        # Create the tables
        query = self.execute("PRAGMA table_info('spatial_ref_sys')")
        if query.fetchone() is None:
            self.execute("SELECT InitSpatialMetaData(1)")
            self.commit()
        self._create_edge_table()
        self._create_node_table()

    def _create_edge_table(self):
        # TODO: covering index = faster lookups. Recreate after loading data.
        sql = [
            "DROP TABLE IF EXISTS edges",
            "CREATE TABLE edges (_u integer, _v integer, _layer text, UNIQUE(_u, _v))",
            "CREATE INDEX edges_u ON edges (_u)",
            "CREATE INDEX edges_v ON edges (_v)",
            "CREATE INDEX edges_uv ON edges (_u, _v)",
        ]
        for s in sql:
            self.execute(s)
        q = self.execute(
            "SELECT * FROM geometry_columns WHERE f_table_name = 'edges' AND f_geometry_column = '_geometry'"
        )
        if q.fetchone() is None:
            self.execute(
                "SELECT AddGeometryColumn('edges', '_geometry', 4326, 'LINESTRING')"
            )
            self.execute("SELECT CreateSpatialIndex('edges', '_geometry')")
        self.commit()

    def _create_node_table(self):
        sql = [
            "DROP TABLE IF EXISTS nodes",
            "CREATE TABLE nodes (_key, UNIQUE(_key))",
            "CREATE INDEX nodes_key ON nodes (_key)",
        ]
        for s in sql:
            self.execute(s)
        q = self.execute(
            "SELECT * FROM geometry_columns WHERE f_table_name = 'nodes' AND f_geometry_column = '_geometry'"
        )
        if q.fetchone() is None:
            self.execute(
                "SELECT AddGeometryColumn('nodes', '_geometry', 4326, 'POINT')"
            )
            self.execute("SELECT CreateSpatialIndex('nodes', '_geometry')")
        self.commit()

    def add_edges(self, ebunch, **attr):
        sql, edges_values, nodes_values = self._prepare_edges_insert(ebunch, **attr)
        self.conn.executemany(sql, edges_values)
        self.conn.executemany(
            "INSERT OR IGNORE INTO nodes (_key, _geometry) VALUES (?, GeomFromText(?, 4326))",
            nodes_values,
        )
        self.conn.commit()

    def add_edges_batched(self, ebunch_to_add, batch_size=10000, **attr):
        ebunch_iter = iter(ebunch_to_add)
        ebunch = []
        while True:
            try:
                edge = next(ebunch_to_add)
                ebunch.append(edge)
            except StopIteration:
                self.add_edges(ebunch, **attr)
                break

            if len(ebunch) == batch_size:
                self.add_edges(ebunch, **attr)
                ebunch = []

    def add_edge(self, _u, _v, ddict):
        self.add_edges(((_u, _v, ddict),))

    def add_nodes(self, nbunch, ddict=None):
        nodes_sql, nodes_values = self._prepare_nodes_insert(nbunch, **attr)
        self.executemany(nodes_sql, nodes_values)
        self.commit()

    def add_nodes_batched(self, nbunch_to_add, batch_size=10000, **attr):
        nbunch_iter = iter(nbunch_to_add)
        nbunch = []
        while True:
            try:
                node = next(nbunch_to_add)
                nbunch.append(node)
            except StopIteration:
                self.add_nodes(nbunch, **attr)
                break

            if len(nbunch) == _batch_size:
                self.add_nodes(nbunch, **attr)
                nbunch = []

    def add_node(self, key, ddict=None):
        self.add_nodes((key), ddict)

    def get_edges_columns(self):
        return [c["name"] for c in self.execute("PRAGMA table_info(edges)")]

    def get_edge_attr(self, u, v):
        q = self.execute(
            "SELECT *, AsGeoJSON(_geometry) _geometry FROM edges WHERE _u = ? AND _v = ?",
            (u, v),
        )
        row = q.fetchone()
        if row is None:
            raise EdgeNotFound("No such edge exists.")

        # Create GeoJSON-like of the geometry
        if row["_geometry"] is not None:
            row["_geometry"] = json.loads(row["_geometry"])

        # Get rid of implementation details
        row.pop("_u")
        row.pop("_v")

        return {key: value for key, value in row.items() if value is not None}

    def get_node(self, key):
        # FIXME: if an error happens here during shortest-path, nx.NodeNotFound is
        # raised. This is not helpful.
        sql = "SELECT *, AsGeoJSON(_geometry) _geometry FROM nodes WHERE _key = ?"
        row = self.execute(sql, (key,)).fetchone()
        if row is None:
            raise NodeNotFound("Specified node does not exist.")
        if row["_geometry"] is not None:
            row["_geometry"] = json.loads(row["_geometry"])
        row.pop("_key")
        return row

    def get_nodes_columns(self):
        return [c["name"] for c in self.execute("PRAGMA table_info(edges)")]

    def has_edge(self, u, v):
        """Test whether an edge exists in the table.

        :param node: Node ID.
        :type node: str

        """
        return self._sql_any(
            "SELECT _u FROM edges WHERE _u = ? AND _v = ? LIMIT 1", (u, v)
        )

    def has_node(self, node):
        """Test whether a node exists in the table.

        :param node: Node ID.
        :type node: str

        """
        return self._sql_any("SELECT _key FROM nodes WHERE _u = ? LIMIT 1", (node,))

    def has_predecessors(self, node):
        """Test whether there are any predecessors for the given node.

        :param node: Node ID.
        :type node: str

        """
        return self._sql_any("SELECT _u FROM edges WHERE _v = ? LIMIT 1")

    def has_successors(self, node):
        """Test whether there are any successors for the given node.

        :param node: Node ID.
        :type node: str

        """
        return self._sql_any("SELECT _u FROM edges WHERE _u = ? LIMIT 1", (node,))

    def iter_edge_ids(self):
        """Create an iterable of edge IDs.

        :returns: Generator of edge IDs.
        :rtype: iterable of (str, str) tuples
        """
        return (
            (row["_u"], row["_v"]) for row in self.execute("SELECT _u, _v FROM edges")
        )

    def iter_edges(self):
        """Create a fast, iterable ebunch (generator of (u, v, d) tuples). The output
        can be used directly as an input for new graphs, including in-memory networkx
        graph instances.

        :returns: Generator of edge data (u, v, d).
        :rtype: iterable of (str, str, dict-like)

        """
        sql = "SELECT *, AsGeoJSON(_geometry) _geometry FROM edges"
        for row in self.execute(sql):
            d = {k: v for k, v in row.items() if v is not None}
            u = d.pop("_u")
            v = d.pop("_v")

            yield (u, v, d)

    def iter_nodes(self):
        """Create a fast, iterable nbunch (generator of (n, d) tuples). The output
        can be used directly as an input for new graphs, including in-memory networkx
        graph instances.

        :returns: Generator of node data (n, d).
        :rtype: iterable of (str, dict-like)

        """
        sql = "SELECT *,  AsGeoJSON(_geometry) _geometry FROM nodes"
        for row in self.execute(sql):
            d = {k: v for k, v in row.items() if v is not None}
            n = d.pop("_key")

            yield (n, d)

    def iter_node_ids(self):
        """Create an iterable of node IDs.

        :returns: Generator of node IDs.
        :rtype: iterable of str
        """
        return (r[0] for r in self.execute("SELECT DISTINCT _key FROM nodes"))

    def iter_predecessor_ids(self, node=None):
        """Create an iterable of all predecessor node IDs: all nodes that are the
        'start' of an edge. If a node is provided, all edges that end on that node are
        queried and their start nodes (i.e. predecessors) are returned.

        :param node: Node ID.
        :type node: str

        """
        if node is None:
            query = self.execute("SELECT DISTINCT _u FROM edges")
        else:
            query = self.execute("SELECT DISTINCT _u FROM edges WHERE _v = ?", (node,))
        return (row["_u"] for row in query)

    def iter_predecessors(self, node):
        """Create an iterable of all predecessors edges of a given node.

        :param node: Node ID.
        :type node: str

        """
        sql = "SELECT *, AsGeoJSON(_geometry) _geometry FROM edges WHERE _v = ?"

        query = self.execute(sql, (node,))

        for row in query:
            # TODO: This is inefficient. Instead, ask for the columns we want rather
            # than discarding results (the _v)
            u = row.pop("_u")
            row.pop("_v")
            yield (u, row)

    def iter_successor_ids(self, node=None):
        """Create an iterable of all successor node IDs: all nodes that are the
        'end' of an edge. If a node is provided, all edges that start on that node are
        queried and their end nodes (i.e. successors) are returned.

        :param node: Node ID.
        :type node: str

        """
        if node is None:
            query = self.execute("SELECT DISTINCT _v FROM edges")
        else:
            query = self.execute("SELECT DISTINCT _v FROM edges WHERE _u = ?", (node,))
        return (row["_v"] for row in query)

    def iter_successors(self, node):
        """Create an iterable of all successor edges of a given node.

        :param node: Node ID.
        :type node: str
        :returns: iterable of (node, edge_attr) pairs representing successors.
        :rtype: generator of 2-tuples

        """
        query = self.execute(
            "SELECT *, AsGeoJSON(_geometry) _geometry FROM edges WHERE _u = ?", (node,)
        )
        for row in query:
            # TODO: This is inefficient. Instead, ask for the columns we want rather
            # than discarding results (the _v)
            v = row.pop("_v")
            row.pop("_u")
            yield (v, row)

    def replace_directed_neighbors(self, node, neighbors, predecessors=False):
        """Create a new set of predecessor or successor edges, replacing any existing
        ones.

        :param node: Node ID for which to replace neighbors (directionally).
        :type node: str
        :param neighbors: An iterable of (node, edge_attr) pairs, where edge_atr is a
                          dict-like.
        :type neighbors: iterable of 2-tuples
        :param predecessors: Whether or not to treat the neighbors as successors
                             (default) or predecessors.
        :type predecessors: bool

        """
        inserts = []
        if predecessors:
            node_col = "_u"
            ebunch = ((n, node, data) for n, data in neighbors)
        else:
            node_col = "_v"
            ebunch = ((node, n, data) for n, data in neighbors)

        # Drop existing neighbors and add new ones
        del_sql = "DELETE FROM edges WHERE {} = ?".format(node_col)
        self.execute(del_sql)
        self.commit()
        self.add_edges(ebunch)

    def replace_predecessors(self, node, predecessors):
        """Create a new set of predecessor edges, replacing any existing ones.

        :param node: Node ID for which to replace predecessors.
        :type node: str
        :param successors: An iterable of (node, edge_attr) pairs, where edge_atr is a
                           dict-like.
        :type successors: iterable of 2-tuples

        """
        self._replace_directed_neighbors(node, successors, predecessors=True)

    def replace_successors(self, node, successors):
        """Create a new set of successor edges, replacing any existing ones.

        :param node: Node ID for which to replace successors.
        :type node: str
        :param successors: An iterable of (node, edge_attr) pairs, where edge_atr is a
                           dict-like.
        :type successors: iterable of 2-tuples

        """
        self._replace_directed_neighbors(node, successors)

    def update_edge(self, u, v, ddict):
        if not ddict:
            return

        # Note: this is just used to create the column type(s) for the new entries
        edges_columns = self.get_edges_columns()
        for key, value in ddict.items():
            if key not in edges_columns:
                sqltype = sqlite_type(value)
                self.execute("ALTER TABLE edges ADD COLUMN {} {}".format(key, sqltype))
                self.commit()

        cols, values = list(zip(*ddict.items()))

        template = "UPDATE edges SET {} WHERE _u = ? AND _v = ?"
        assignments = ", ".join([c + "=?" for c in cols])
        sql = template.format(assignments)

        self.execute(sql, list(values) + [u, v])
        self.commit()

    def update_edges(self, ebunch):
        """Update a bunch of edges at once. Update means the u, v IDs already exist and
           only the attributes need to be updated / created. Calls UPDATE in SQL.

        """
        edges_columns = self.get_edges_columns()
        for u, v, d in ebunch:
            # Add any missing column types based on first occurrence
            for key, value in d.items():
                if key not in edges_columns:
                    sqltype = sqlite_type(value)
                    self.execute(
                        "ALTER TABLE edges ADD COLUMN {} {}".format(key, sqltype)
                    )
                    self.commit()
                    edges_columns.append(key)

        template = "UPDATE edges SET {} WHERE _u = ? AND _v = ?"

        for u, v, d in ebunch:
            cols, values = list(zip(*d.items()))
            assignments = ", ".join([c + "=?" for c in cols])
            sql = template.format(assignments)
            self.execute(sql, list(values) + [u, v])

        self.commit()

    def update_node(self, key, ddict):
        if not ddict:
            return

        cols, values = self._prepare_nodes((key, ddict))

        template = "UPDATE nodes SET {} WHERE _key = ?"
        assignments = ", ".join([c + "=?" for c in cols])
        sql = template.format(assignments)

        self.execute(sql, list(values) + [u])
        self.commit()

    def _prepare_edges(self, ebunch, nodes=False, **attr):
        # TODO: also return the columns to create, if any, rather than actually
        # creating them in the table. If an error occurs and the edges can't be
        # inserted/updated, the table schema should not be changed.
        edges_columns = self.get_edges_columns()
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
            edge_id = (_u, _v)
            in_db = self._sql_any(
                "SELECT _u FROM edges WHERE _u = ? AND _v = ?", edge_id
            )
            if in_db or edge_id in seen:
                continue

            # TODO: convert to WKT at this step rather than i/o?
            values = [_u, _v]
            # Skip first two columns - these are _u and _v and we already
            # accounted for them
            for c in edges_columns[2:]:
                try:
                    value = d.pop(c)
                except KeyError:
                    value = None
                values.append(value)
            if d:
                # There are new columns!
                for key, value in d.items():
                    sqltype = sqlite_type(value)
                    self.execute(
                        "ALTER TABLE edges ADD COLUMN {} {}".format(key, sqltype)
                    )
                    self.commit()
                    edges_columns.append(key)
                    values.append(value)

            edges_values.append(values)

            # TODO: might save some time by not doing redundant node creation
            # code (check if the node already exists)
            if nodes:
                for node in (_u, _v):
                    node_geom = "POINT(" + " ".join(node.split(", ")) + ")"
                    nodes_values.append((node, node_geom))

            seen.add((_u, _v))

        if nodes:
            return edges_columns, edges_values, nodes_values
        else:
            return edges_columns, edges_values

    def _prepare_edges_insert(self, ebunch, **attr):
        edges_columns, edges_values, nodes_values = self._prepare_edges(
            ebunch, nodes=True, **attr
        )

        placeholders = [
            GEOM_PLACEHOLDER if c == "_geometry" else PLACEHOLDER for c in edges_columns
        ]
        edges_template = "INSERT OR IGNORE INTO edges ({}) VALUES ({})"
        edges_sql = edges_template.format(
            ", ".join(edges_columns), ", ".join(placeholders)
        )

        return edges_sql, edges_values, nodes_values

    def _prepare_nodes(self, nbunch, **attr):
        # TODO: also return the columns to create, if any, rather than actually
        # creating them in the table. If an error occurs and the edges can't be
        # inserted/updated, the table schema should not be changed.
        columns = self.get_nodes_columns()

        values = []
        seen = set([])
        for node in nbunch:
            if len(node) == 1:
                _key = node[0]
                d = attr
            elif len(node) == 2:
                _key = node[0]
                d = {**attr, **edge[2]}
            else:
                # TODO: this doesn't seem useful. Skip + warn / raise other
                # error?
                raise ValueError(
                    "Node must be 1-tuple of (key,) or 2-tuple of (key, d)"
                )

            # Check for edge already existing. Skip.
            # TODO: Issue a warning?
            if self.has_node(_key) or edge_id in seen:
                continue

            # TODO: convert to WKT at this step rather than i/o?
            values = [_key]
            # Skip first two columns - these are _u and _v and we already
            # accounted for them
            for c in columns[2:]:
                try:
                    value = d.pop(c)
                except KeyError:
                    value = None
                values.append(value)
            if d:
                # There are new columns!
                for key, value in d.items():
                    sqltype = sqlite_type(value)
                    self.execute(
                        "ALTER TABLE nodes ADD COLUMN {} {}".format(key, sqltype)
                    )
                    self.commit()
                    columns.append(key)
                    values.append(value)

            nodes_values.append(values)

            seen.add((_u, _v))

        return nodes_columns, nodes_values

    def _prepare_nodes_insert(self, nbunch, **attr):
        nodes_columns, nodes_values = self._prepare_nodes(ebunch, **attr)

        placeholders = [
            GEOM_PLACEHOLDER if c == "_geometry" else PLACEHOLDER for c in nodes_columns
        ]
        nodes_template = "INSERT OR IGNORE INTO nodes ({}) VALUES ({})"
        nodes_sql = nodes_template.format(
            ", ".join(nodes_columns), ", ".join(placeholders)
        )

        return nodes_sql, nodes_values

    def _sql_any(self, sql, values):
        if self.execute(sql, values).fetchone() is not None:
            return True
        return False
