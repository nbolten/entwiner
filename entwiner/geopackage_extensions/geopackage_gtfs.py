from collections import OrderedDict

import partridge as ptg

from ..geopackage.feature_table import FeatureTable


class GTFSStops(FeatureTable):
    @classmethod
    def from_path(cls, path, gpkg):
        table = cls(gpkg, "gtfs_stops", "POINT", srid=4326)
        table.create_tables()
        gpkg.feature_tables["gtfs_stops"] = table

        feed = ptg.load_feed(path)

        def extract_row(row):
            d = OrderedDict()
            d["geom"] = {
                "type": "Point",
                "coordinates": (row["stop_lon"], row["stop_lat"]),
            }
            for key in ("stop_id", "stop_name"):
                d[key] = row[key]

            return d

        features = feed.stops.apply(extract_row, axis=1)
        table.write_features(features)

        return table
