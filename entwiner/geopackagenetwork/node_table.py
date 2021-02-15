from ..exceptions import NodeNotFound
from ..geopackage.feature_table import FeatureTable


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
