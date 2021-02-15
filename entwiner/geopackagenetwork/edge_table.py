from ..geopackage.feature_table import FeatureTable


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
