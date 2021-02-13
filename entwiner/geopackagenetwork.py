from collections import OrderedDict
import sqlite3

from .geopackage import GeoPackage, FeatureTable
from .exceptions import NodeNotFound


class EdgeTable(FeatureTable):
    def write_features(self, features, batch_size=10_000, counter=None):
        # FIXME: should fill a nodes queue instead of realizing a full list at
        # this step
        ways_queue = []
        nodes_queue = []

        for feature in features:
            if len(ways_queue) >= batch_size:
                super().write_features(ways_queue, 10_000, counter)
                self.gpkg.feature_tables["nodes"].write_features(nodes_queue)
                ways_queue = []
                nodes_queue = []
            ways_queue.append(feature)
            u_feature = {"_n": feature["_u"]}
            v_feature = {"_n": feature["_v"]}
            if self.geom_column in feature:
                u_feature[self.geom_column] = {
                    "type": "Point",
                    "coordinates": feature["geom"]["coordinates"][0],
                }
                v_feature[self.geom_column] = {
                    "type": "Point",
                    "coordinates": feature["geom"]["coordinates"][-1],
                }
            nodes_queue.append(u_feature)
            nodes_queue.append(v_feature)

        self.gpkg.feature_tables["nodes"].write_features(nodes_queue)
        super().write_features(ways_queue, batch_size, counter)

    def dwithin(self, lon, lat, distance, sort=False):
        rows = super().dwithin(lon, lat, distance, sort=sort)
        return (self._graph_format(row) for row in rows)

    def update(self, ebunch):
        with self.gpkg.connect() as conn:
            fids = []
            # TODO: investigate whether this is a slow step
            for u, v, d in ebunch:
                # TODO: use different column format for this step? No need to
                # get a dictionary as query output first.
                fid = conn.execute(
                    f"SELECT fid FROM {self.name} WHERE _u = ? AND _v = ?",
                    (u, v),
                ).fetchone()["fid"]
                fids.append(fid)

        ddicts = []
        for u, v, d in ebunch:
            ddict = self.serialize_row(next(self._table_format(((u, v, d),))))
            ddicts.append(ddict)

        super().update_batch(zip(fids, ddicts))

    def successor_nodes(self, n=None):
        with self.gpkg.connect() as conn:
            if n is None:
                rows = conn.execute(f"SELECT DISTINCT _v FROM {self.name}")
            else:
                rows = conn.execute(
                    f"SELECT _v FROM {self.name} WHERE _u = ?", (n,)
                )
            # TODO: performance increase by temporary changing row handler?
            ns = [r["_v"] for r in rows]
        return ns

    def predecessor_nodes(self, n=None):
        with self.gpkg.connect() as conn:
            if n is None:
                rows = conn.execute(f"SELECT DISTINCT _u FROM {self.name}")
            else:
                rows = conn.execute(
                    f"SELECT _u FROM {self.name} WHERE _v = ?", (n,)
                )
            # TODO: performance increase by temporary changing row handler?
            ns = [r["_u"] for r in rows]
        return ns

    def successors(self, n):
        with self.gpkg.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM {self.name} WHERE _u = ?", (n,)
            )
            # TODO: performance increase by temporary changing row handler?
            ns = []
            for r in rows:
                u, v, d = self._graph_format(r)
                ns.append((v, self.deserialize_row(d)))
        return ns

    def predecessors(self, n):
        with self.gpkg.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM {self.name} WHERE _v = ?", (n,)
            )
            # TODO: performance increase by temporary changing row handler?
            ns = [(r.pop("_u"), r) for r in rows]
        return ns

    def unique_predecessors(self, n=None):
        with self.gpkg.connect() as conn:
            if n is None:
                rows = conn.execute(
                    f"SELECT COUNT(DISTINCT(_u)) c FROM {self.name}"
                )
            else:
                rows = conn.execute(
                    f"""
                    SELECT COUNT(DISTINCT(_u)) c FROM {self.name} WHERE _v = ?
                """,
                    (n,),
                )
            count = next(rows)["c"]
        return count

    def unique_successors(self, n=None):
        with self.gpkg.connect() as conn:
            if n is None:
                rows = conn.execute(
                    f"SELECT COUNT(DISTINCT(_v)) c FROM {self.name}"
                )
            else:
                rows = conn.execute(
                    f"""
                    SELECT COUNT(DISTINCT(_u)) c FROM {self.name} WHERE _u = ?
                """,
                    (n,),
                )
            count = next(rows)["c"]
        return count

    def get_edge(self, u, v):
        with self.gpkg.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {self.name} WHERE _u = ? AND _v = ?
            """,
                (u, v),
            )
            # TODO: performance increase by temporary changing row handler?
            return self.deserialize_row(next(rows))

    @staticmethod
    def _graph_format(row):
        u = row.pop("_u")
        v = row.pop("_v")
        return u, v, row

    @staticmethod
    def _table_format(ebunch):
        for u, v, d in ebunch:
            ddict = {"_u": u, "_v": v, **d}
            if "fid" in ddict:
                ddict.pop("fid")
            yield ddict

    def __iter__(self):
        for row in super().__iter__():
            yield self._graph_format(row)


class NodeTable(FeatureTable):
    def dwithin(self, lon, lat, distance, sort=False):
        rows = super().dwithin(lon, lat, distance, sort=sort)
        return (self._graph_format(row) for row in rows)

    def update(self, ebunch):
        super().update(self.serialize_row(self._table_format(ebunch)))

    def get_node(self, n):
        with self.gpkg.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {self.name} WHERE _n = ?
            """,
                (n,),
            )
            # TODO: performance increase by temporary changing row handler?
            try:
                return self.deserialize_row(next(rows))
            except StopIteration:
                raise NodeNotFound()

    @staticmethod
    def _graph_format(row):
        n = row.pop("_n")
        return n, row

    @staticmethod
    def _table_format(ebunch):
        for n, d in ebunch:
            yield {"_n": n, **d}

    def __iter__(self):
        for row in super().__iter__():
            yield self._graph_format(row)


class GeoPackageNetwork:
    def __init__(self, path=None, srid=4326):
        self.path = path
        self.gpkg = GeoPackage(path=path)
        # TODO: handle reprojection during addition of features
        self.srid = srid

        # TODO: handle recognition of existing geopackage (with expected
        #       tables) vs. initializing one from scratch.
        self._create_graph_tables()
        self.edges = EdgeTable(self.gpkg, "edges", "LINESTRING", srid=srid)
        self.nodes = NodeTable(self.gpkg, "nodes", "POINT", srid=srid)
        self.gpkg.feature_tables["edges"] = self.edges
        self.gpkg.feature_tables["nodes"] = self.nodes

    def copy(self, path):
        self.gpkg.copy(path)
        return GeoPackageNetwork(path, srid=self.srid)

    def _create_graph_tables(self):
        # TODO: consider creating metadata table to support multiple
        #       feature_tables, create edges view? Benchmark performance.
        #       Should be ~2X slowdown, but is more flexible and smaller
        #       change, easier to add/remove from a GeoPackage.
        try:
            with self.gpkg.connect() as conn:
                edges_table_query = conn.execute(
                    """
                    SELECT table_name FROM gpkg_contents WHERE table_name = 'edges'
                """
                )
                next(edges_table_query)
        except StopIteration:
            self.gpkg.add_feature_table("edges", "LINESTRING", self.srid)

        try:
            with self.gpkg.connect() as conn:
                nodes_table_query = conn.execute(
                    """
                    SELECT table_name FROM gpkg_contents WHERE table_name = 'nodes'
                """
                )
                next(nodes_table_query)
        except StopIteration:
            self.gpkg.add_feature_table("nodes", "POINT", self.srid)

        with self.gpkg.connect() as conn:
            try:
                conn.execute("ALTER TABLE nodes ADD _n TEXT")
                conn.execute("ALTER TABLE edges ADD _u TEXT")
                conn.execute("ALTER TABLE edges ADD _v TEXT")
            except sqlite3.OperationalError:
                # Ignore case where columns already exist
                pass
        with self.gpkg.connect() as conn:
            # NOTE: create these indices later to improve performance?
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS nodes_n_index
                                         ON nodes (_n)
            """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS edges_u_index ON edges (_u)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS edges_v_index ON edges (_v)"
            )
            conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS edges_uv_index
                                                           ON edges (_u, _v)
            """
            )

    def add_edges(self, edges, batch_size=10_000, **attr):
        """Add edges to the network.

        :param edges: an iterable of 2-tuples or 3-tuples representing (u, v)
                      or (u, v, d) edges (as expected by NetworkX). Iterable
                      can mix both edge types.
        :type edges: iterable
        :param batch_size: Size of batches to write downstream.
        :type batch_size: int
        :param attr: Any default attributes to add to all edges. If any
                     attributes conflict with edge data, edge data supercedes.
        :type attr: dict

        """
        node_queue = []
        edge_queue = []

        for edge in edges:
            if len(edge_queue) > batch_size:
                self.edges.write_features(edge_queue, batch_size=batch_size)
                self.nodes.write_features(node_queue, batch_size=batch_size)
                node_queue = []
                edge_queue = []

            edge_data = {}
            try:
                u, v = edge
            except (TypeError, ValueError):
                try:
                    u, v, edge_data = edge
                except (TypeError, ValueError):
                    raise ValueError(
                        "Edge must be 2-tuple of (u, v) or 3-tuple of "
                        "(u, v, d)"
                    )

            d = OrderedDict()
            d["_u"] = u
            d["_v"] = v
            for key, value in edge_data.items():
                d[key] = value

            for key, value in attr.items():
                d[key] = value

            edge_queue.append(d)

            if "geom" in d:
                node_queue.append(
                    OrderedDict(
                        (
                            ("_n", u),
                            (
                                "geom",
                                {
                                    "type": "Point",
                                    "coordinates": d["geom"]["coordinates"][0],
                                },
                            ),
                        )
                    )
                )
                node_queue.append(
                    OrderedDict(
                        (
                            ("_n", v),
                            (
                                "geom",
                                {
                                    "type": "Point",
                                    "coordinates": d["geom"]["coordinates"][
                                        -1
                                    ],
                                },
                            ),
                        )
                    )
                )

        self.edges.write_features(edge_queue, batch_size=batch_size)
        self.nodes.write_features(node_queue, batch_size=batch_size)
