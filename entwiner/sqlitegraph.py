import json
import sqlite3

from shapely.geometry import shape, Point

from .geom import wkt_linestring, wkt_point
from .utils import sqlite_type
from .exceptions import EdgeNotFound, NodeNotFound

PLACEHOLDER = "?"
GEOM_PLACEHOLDER = "GeomFromText(?, 4326)"


class SQLiteGraph:
    def __init__(self, path):
        self.path = path
        self.conn = self.connect()

    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = _dict_factory
        # conn.row_factory = sqlite3.Row
        conn.enable_load_extension(True)
        conn.load_extension("mod_spatialite.so")
        return conn

    def execute(self, sql, values=(), commit=False):
        result = self.conn.execute(sql, values)
        if commit:
            self.commit()
        return result

    def executemany(self, sql, values):
        return self.conn.executemany(sql, values)

    def commit(self):
        return self.conn.commit()

    def reindex(self):
        edges_index = _sql_column_list(self.get_columns("edges"))
        nodes_index = _sql_column_list(self.get_columns("nodes"))
        self.execute("DROP INDEX IF EXISTS edges_covering")
        self.execute(f"CREATE INDEX edges_covering ON edges ({edges_index})")
        self.execute("DROP INDEX IF EXISTS nodes_covering")
        self.execute(f"CREATE INDEX nodes_covering ON nodes ({nodes_index})")
        self.commit()

    def to_in_memory(self):
        # Load into new memory-based DB
        new_db = SQLiteGraph(":memory:")
        # Replace database connection with clean one so that values can be directly
        # dumped.
        new_db.conn = sqlite3.connect(":memory:")
        new_db.conn.enable_load_extension(True)
        new_db.conn.load_extension("mod_spatialite.so")

        row_factory = self.conn.row_factory
        self.conn.row_factory = None

        for line in self.conn.iterdump():
            # Skip all index creation - these should be recreated afterwards
            if "CREATE TABLE" in line or "INSERT INTO" in line:
                if "idx_" in line:
                    continue
            if "COMMIT" in line:
                continue
            new_db.conn.cursor().executescript(line)
            new_db.conn.commit()
        self.conn.row_factory = row_factory

        new_db.conn.execute(
            "SELECT RecoverGeometryColumn('edges', '_geometry', 4326, 'LINESTRING')"
        )
        # TODO: Not necessary?
        new_db.conn.execute("SELECT DisableSpatialIndex('edges', '_geometry')")
        new_db.conn.execute("DROP TABLE IF EXISTS idx_edges__geometry")
        new_db.conn.execute("SELECT CreateSpatialIndex('edges', '_geometry')")

        new_db.conn.row_factory = row_factory

        return new_db

    def _create_graph(self):
        # Create the tables
        query = self.execute("PRAGMA table_info('spatial_ref_sys')")
        if query.fetchone() is None:
            self.execute("SELECT InitSpatialMetaData(1)")
            self.commit()
        self._create_edge_table()
        self._create_node_table()

    def _create_edge_table(self):
        sql = (
            "DROP TABLE IF EXISTS edges",
            "CREATE TABLE edges (_u integer, _v integer, _layer text, UNIQUE(_u, _v))",
            "CREATE INDEX edges_u ON edges (_u)",
            "CREATE INDEX edges_v ON edges (_v)",
            "CREATE UNIQUE INDEX edges_uv ON edges (_u, _v)",
        )
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
        sql = (
            "DROP TABLE IF EXISTS nodes",
            "CREATE TABLE nodes (_n, UNIQUE(_n))",
            "CREATE INDEX nodes_n ON nodes (_n)",
        )
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

    def insert_or_replace_edge(self, u, v, d, commit=False):
        cols, vals = zip(*d.items())
        cols = ("_u", "_v", *cols)

        placeholders = _sql_column_placeholders(cols)
        columns_string = _sql_column_list(cols)
        vals = (u, v, *vals)
        sql = f"REPLACE INTO edges ({columns_string}) VALUES ({placeholders})"
        self.execute(sql, vals, commit=commit)

    def add_edges(self, ebunch, **attr):
        for edge in ebunch:
            if len(edge) == 2:
                u, v = edge
                d = attr
            elif len(edge) == 3:
                u = edge[0]
                v = edge[1]
                d = {**attr, **edge[2]}
            else:
                raise ValueError(
                    "Edge must be 2-tuple of (u, v) or 3-tuple of (u, v, d)"
                )

            if "_geometry" in d:
                # Convert to wkt strings for insertion
                # TODO: create forward/reverse serialization methods that are
                # reusable.
                lon_u, lat_u = d["_geometry"]["coordinates"][0]
                lon_v, lat_v = d["_geometry"]["coordinates"][-1]
                u_wkt = wkt_point(lon_u, lat_u)
                v_wkt = wkt_point(lon_v, lat_v)
                d["_geometry"] = wkt_linestring(d["_geometry"]["coordinates"])

            self._add_columns_if_new_keys("edges", d, commit=False)
            self.insert_or_replace_edge(u, v, d, commit=False)

            # TODO: allow overriding node IDs during insert of edges, e.g. when
            # they are already stored as attributes in OpenStreetMap way data.
            if "_geometry" in d:
                self.add_node(u, ndict={"_geometry": u_wkt}, commit=False)
                self.add_node(v, ndict={"_geometry": v_wkt}, commit=False)
            else:
                self.add_node(u, commit=False)
                self.add_node(v, commit=False)

        self.commit()

    def add_edge(self, u, v, ddict):
        self.add_edges(((u, v, ddict),))

    def add_edges_batched(self, ebunch_to_add, batch_size=10000, counter=None, **attr):
        ebunch_iter = iter(ebunch_to_add)
        ebunch = []
        while True:
            try:
                edge = next(ebunch_to_add)
                ebunch.append(edge)
            except StopIteration:
                self.add_edges(ebunch, **attr)
                if counter is not None:
                    counter.update(len(ebunch))
                break

            if len(ebunch) == batch_size:
                self.add_edges(ebunch, **attr)
                if counter is not None:
                    counter.update(batch_size)
                ebunch = []

    def add_node(self, n, ndict=None, commit=True):
        keys, values = zip(*ndict.items())

        columns = _sql_column_list(("_n", *keys))
        placeholders = _sql_column_placeholders((n, *keys))
        values = (n, *values)

        sql = f"REPLACE INTO nodes ({columns}) VALUES ({placeholders})"
        self.execute(sql, values)

        if commit:
            self.commit()

    def add_nodes(self, nbunch, **attr):
        for n in nbunch:
            if type(n) == str:
                # It's just a single node ID
                ndict = {**attr}
            else:
                # It's a (n, ndict) tuple
                n, ndict = n
                if attr:
                    ndict = {**attr, **ndict}

            self.add_node(n, ndict, commit=False)

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

    def get_columns(self, table_name):
        return list(c["name"] for c in self.execute(f"PRAGMA table_info({table_name})"))

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

    def get_node(self, n):
        sql = "SELECT *, AsGeoJSON(_geometry) _geometry FROM nodes WHERE _n = ?"
        row = self.execute(sql, (n,)).fetchone()
        if row is None:
            raise NodeNotFound("Specified node does not exist.")
        if row["_geometry"] is not None:
            row["_geometry"] = json.loads(row["_geometry"])
        row.pop("_n")
        return row

    def has_edge(self, u, v):
        """Test whether an edge exists in the table.

        :param node: Node ID.
        :type node: str

        """
        return self._sql_any(
            "SELECT _u FROM edges WHERE _u = ? AND _v = ? LIMIT 1", (u, v)
        )

    def has_node(self, n):
        """Test whether a node exists in the table.

        :param node: Node ID.
        :type node: str

        """
        return self._sql_any("SELECT _n FROM nodes WHERE _u = ? LIMIT 1", (n,))

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

    def delete_edges(self, edge_ids):
        """Delete nodes from nodes table.

        :param edge_ids: Iterable of (u, v) edge IDs.
        :type edge_ids: iterable of 2-tuples.

        """
        sql = f"DELETE FROM edges WHERE _u = ? AND _v = ?"
        self.executemany(sql, edge_ids)

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
            n = d.pop("_n")

            yield (n, d)

    def iter_node_ids(self):
        """Create an iterable of node IDs.

        :returns: Generator of node IDs.
        :rtype: iterable of str

        """
        return (r[0] for r in self.execute("SELECT DISTINCT _n FROM nodes"))

    def delete_nodes(self, nbunch):
        """Delete nodes from nodes table.

        :param node_ids: An iterable of node IDs.
        :type node_ids: Iterable of str.

        """
        sql = f"DELETE FROM nodes WHERE _n = ?"
        self.executemany(sql, ((n,) for n in nbunch))

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
        del_sql = f"DELETE FROM edges WHERE {node_col} = ?"
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

    def set_node_attr(self, n, key, value):
        self._add_columns_if_new_keys("nodes", {key: value})

        sql = f"UPDATE nodes SET {key}=? WHERE _n = ?"

        self.execute(sql, (value, n))
        self.commit()

    def update_node(self, n, ddict):
        if not ddict:
            return

        self._add_columns_if_new_keys(self, "nodes", ddict)
        cols, values = zip(*ddict.items())

        sql = self._update_node_sql(cols)
        self.execute(sql, (*values, n))
        self.commit()

    def set_edge_attr(self, u, v, key, value):
        self._add_columns_if_new_keys(self, "edges", {key: value})

        sql = f"UPDATE edges SET {key}=? WHERE _u = ? AND _v = ?"

        self.execute(sql, (value, u, v))
        self.commit()

    def update_edge(self, u, v, ddict, commit=True):
        if not ddict:
            return

        self._add_columns_if_new_keys(self, "edges", ddict)
        cols, values = zip(*ddict.items())

        sql = self._update_edge_sql(cols)
        self.execute(sql, (*values, u, v))
        if commit:
            self.commit()

    @staticmethod
    def _update_edge_sql(cols):
        column_template = _sql_set_column_list(columns)
        sql = f"UPDATE edges SET {column_template} WHERE _u = ? AND _v = ?"
        return sql

    @staticmethod
    def _update_node_sql(cols):
        column_template = _sql_set_column_list(columns)
        sql = f"UPDATE nodes SET {column_template} WHERE _node = ?"
        return sql

    def update_edges(self, ebunch):
        """Update a bunch of edges at once. Update means the u, v IDs already exist and
           only the attributes need to be updated / created. Calls UPDATE in SQL.

        """
        for u, v, d in ebunch:
            self.update_edge(u, v, d, commit=False)

        self.commit()

    def _add_columns_if_new_keys(self, table_name, ddict, commit=False):
        """Add columns to a table given a stream of key:value pairs. Keys will become
        column names and values will determine the column type

        :param table_name: name of the table to which to add columns.
        :type table_name: str
        :param items: iterable of (key, value) tuples, identical to output of
                      dict.items(). Keys will become column names, and value types
                      will dictate column types.
        :type items: iterable of (key, value) pairs

        """
        columns = self.get_columns(table_name)
        for key, value in ddict.items():
            if key not in columns:
                sqltype = sqlite_type(value)
                self.execute(f"ALTER TABLE {table_name} ADD COLUMN '{key}' {sqltype}")
        if commit:
            self.commit()

    def _sql_any(self, sql, values):
        if self.execute(sql, values).fetchone() is not None:
            return True
        return False

    def edges_dwithin(self, lon, lat, distance, sort=False):
        """Finds edges within some distance of a point.

        :param lon: The longitude of the query point.
        :type lon: float
        :param lat: The latitude of the query point.
        :type lat: float
        :param distance: distance from point to search ('DWithin').
        :type distance: float
        :param sort: Sort the results by distance (nearest first).
        :type sort: bool
        :returns: Generator of copies of edge data (represented as dicts).
        :rtype: generator of dicts

        """
        # TODO: use legit distance and/or projected data, not lon-lat
        rtree_sql = """
            SELECT rowid
              FROM SpatialIndex
             WHERE f_table_name = 'edges'
               AND search_frame = BuildMbr(?, ?, ?, ?, 4326)
        """
        point = Point(lon, lat)

        bbox = (lon - distance, lat - distance, lon + distance, lat + distance)

        index_query = self.execute(rtree_sql, bbox)
        rowids = ", ".join(str(r["rowid"]) for r in index_query)

        # TODO: put fast rowid-based lookup in G.sqlitegraph object.
        query = self.execute(
            f"""
            SELECT rowid, *, AsGeoJSON(_geometry) _geometry
              FROM edges
             WHERE rowid IN ({rowids})
        """
        )

        if sort:
            return sorted(query, key=lambda r: _distance_sort(r, point))

        return (r for r in query)


def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _sql_column_list(columns):
    return ", ".join(f"'{c}'" for c in columns)


def _sql_set_column_list(columns):
    return ", ".join(
        f"'{c}'={GEOM_PLACEHOLDER if c == '_geometry' else PLACEHOLDER}"
        for c in columns
    )


def _sql_column_placeholders(columns):
    return ", ".join(
        GEOM_PLACEHOLDER if c == "_geometry" else PLACEHOLDER for c in columns
    )


def _distance_sort(row, point):
    geometry = shape(json.loads(r["_geometry"]))
    return geometry.distance(point)
