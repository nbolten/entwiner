from collections import OrderedDict

import geomet.wkb
import pyproj
from shapely.geometry import LineString, Point, shape
from shapely.ops import transform

from ..utils import haversine


GPKG_APPLICATION_ID = 1196444487
GPKG_USER_VERSION = 10200
COL_TYPE_MAP = {
    str: "TEXT",
    int: "INTEGER",
    float: "DOUBLE",
}

# FIXME: don't hardcore 26910, discover an appropriate projection based on
# data. NOTE: this entire strategy is based around having no function to
# calculate the distance (meters) between a LineString and a point directly. If
# such a function were implemented, reprojection would be unnecessary.
TO_SRID = 3740


class FeatureTable:
    geom_column = "geom"
    primary_key = "fid"

    def __init__(self, gpkg, name, geom_type, srid=4326):
        self.gpkg = gpkg
        self.name = name
        self.geom_type = geom_type
        self.srid = srid

        self.add_srs()

        self.transformer = pyproj.Transformer.from_crs(
            f"epsg:{self.srid}", f"epsg:{TO_SRID}", always_xy=True
        )

    def create_tables(self):
        """Initialize the feature_table's tables, as they do not yet exist."""
        # TODO: implement enum for geom_type?
        # TODO: implement 'last change' column logic for gpkg_contents.
        # FIXME: Catch cases where these tables don't exist, raise useful
        #        exception. Should indicate a bad GeoPackage.
        # TODO: catch case where feature_table has already been added
        with self.gpkg.connect() as conn:
            conn.execute(
                """
                INSERT INTO gpkg_contents
                            (
                                table_name,
                                data_type,
                                identifier,
                                srs_id
                            )
                     VALUES (
                         ?,
                         'features',
                         ?,
                         ?
                     )
            """,
                (self.name, self.name, self.srid),
            )

            # TODO: implement 'feature_count' column logic for
            #       gpkg_ogr_contents.
            conn.execute(
                "INSERT INTO gpkg_ogr_contents ( table_name ) VALUES ( ? )",
                (self.name,),
            )

            conn.execute(
                f"""
                INSERT INTO gpkg_geometry_columns
                            (
                                table_name,
                                column_name,
                                geometry_type_name,
                                srs_id,
                                z,
                                m
                            )
                     VALUES (?, ?, ?, ?, ?, ?)
            """,
                (self.name, self.geom_column, self.geom_type, self.srid, 0, 0),
            )
            conn.execute(
                f"""
                CREATE TABLE {self.name} (
                    {self.primary_key} INTEGER,
                    {self.geom_column} TEXT,
                    PRIMARY KEY ({self.primary_key})
                )
            """
            )

    def drop_tables(self):
        with self.gpkg.connect() as conn:
            conn.execute(
                "DELETE FROM gpkg_contents WHERE table_name = ?", (self.name,)
            )
            # TODO: implement 'feature_count' column logic for
            #       gpkg_ogr_contents.
            conn.execute(
                "DELETE FROM gpkg_ogr_contents WHERE table_name = ?",
                (self.name,),
            )

            conn.execute(
                "DELETE FROM gpkg_geometry_columns WHERE table_name = ?",
                (self.name,),
            )

            conn.execute(f"DROP TABLE {self.name}")

    def intersects(self, left, bottom, right, top):
        """Finds features intersecting a bounding box.

        :param left: left coordinate of bounding box.
        :type left: float
        :param bottom: bottom coordinate of bounding box.
        :type bottom: float
        :param right: right coordinate of bounding box.
        :type right: float
        :param top: top coordinate of bounding box.
        :type top: float
        :returns: Generator of copies of edge data (represented as dicts).
        :rtype: generator of dicts

        """
        with self.gpkg.connect() as conn:
            rtree_rows = conn.execute(
                f"""
                SELECT id
                  FROM rtree_{self.name}_{self.geom_column}
                 WHERE maxX >= ?
                   AND minX <= ?
                   AND maxY >= ?
                   AND minY <= ?
            """,
                (left, right, bottom, top),
            )
            ids = [r["id"] for r in rtree_rows]

        rows = []
        with self.gpkg.connect() as conn:
            for i in ids:
                row = conn.execute(
                    f"""
                    SELECT *
                      FROM {self.name}
                     WHERE {self.primary_key} = ?
                """,
                    (i,),
                )
                row = self.deserialize_row(next(row))
                rows.append(row)
        return rows

    def dwithin_rtree(self, lon, lat, distance):
        """Finds features within some distance of a point using a bounding box.
        Includes all entries within the bounding box, not just those within the
        exact distance.

        :param lon: The longitude of the query point.
        :type lon: float
        :param lat: The latitude of the query point.
        :type lat: float
        :param distance: distance from point to search ('DWithin').
        :type distance: float
        :returns: Generator of copies of edge data (represented as dicts).
        :rtype: generator of dicts

        """
        # NOTE: Because these transformations are each in one dimension,
        #       reprojection is not necessary. Can just translate from lon to
        #       meters, lat to meters separately.
        x, y = self.transformer.transform(lon, lat)

        left = x - distance
        bottom = y - distance
        right = x + distance
        top = y + distance

        left, bottom = self.transformer.transform(
            left, bottom, direction=pyproj.enums.TransformDirection.INVERSE
        )
        right, top = self.transformer.transform(
            right, top, direction=pyproj.enums.TransformDirection.INVERSE
        )

        return self.intersects(left, bottom, right, top)

    def dwithin(self, lon, lat, distance, sort=False):
        """Finds features within some distance of a point using a bounding box.

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
        # FIXME: check for existence of rtree and if it doesn't exist, raise
        #        custom exception. Repeat for all methods that refer to rtree.
        rows = self.dwithin_rtree(lon, lat, distance)
        # Note that this sorting strategy is inefficient, sorting the entire
        # result and not using any distance-based tricks for optimal
        # spitting-out of edges.
        # TODO: Implement rtree-inspired real distance sort method using
        #       minheap
        distance_rows = []

        for r in rows:
            x, y = self.transformer.transform(lon, lat)
            point2 = Point(x, y)
            ls = shape(r[self.geom_column])
            line2 = transform(self.transformer.transform, ls)
            distance_between = point2.distance(line2)
            distance_rows.append((r, distance_between))

        if sort:
            sorted_by_distance = sorted(distance_rows, key=lambda r: r[1])
            return (r for r, d in sorted_by_distance if d < distance)

        return (r for r, d in distance_rows if d < distance)

    def update_batch(self, bunch):
        bunch = list(bunch)
        primary_keys, ddicts = zip(*bunch)
        # TODO: wrap new columns + inserts in a single transaction?
        self._add_new_columns(ddicts)
        column_names = self._get_column_names()
        with self.gpkg.connect() as conn:
            for primary_key, ddict in bunch:
                set_columns = []
                set_values = []
                for column_name in column_names:
                    if column_name in ddict:
                        set_columns.append(column_name)
                        set_values.append(ddict[column_name])

                set_clauses = ", ".join([f"{c} = ?" for c in set_columns])
                conn.execute(
                    f"""
                    UPDATE {self.name}
                       SET {set_clauses}
                     WHERE {self.primary_key} = ?
                """,
                    (*set_values, primary_key),
                )

    def update(self, primary_key, ddict):
        self.update_batch(((primary_key, ddict),))

    def add_rtree(self):
        with self.gpkg.connect() as conn:
            rtree_table = f"rtree_{self.name}_{self.geom_column}"
            conn.execute(
                f"""
                INSERT INTO gpkg_extensions
                            (
                                table_name,
                                column_name,
                                extension_name,
                                definition,
                                scope
                            )
                     SELECT ?,
                            '{self.geom_column}',
                            'gpkg_rtree_index',
                            'http://www.geopackage.org/spec120/#extension_rtree',
                            'write-only'
                     WHERE NOT EXISTS(
                        SELECT 1
                          FROM gpkg_extensions
                         WHERE extension_name = 'gpkg_rtree_index'
                     )
            """,
                (self.name,),
            )

            conn.execute(
                f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {rtree_table} USING rtree(
                    id,
                    minX, maxX,
                    minY, maxY,
                )
            """
            )
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {rtree_table}
                     SELECT {self.primary_key} id,
                            MbrMinX({self.geom_column}) minX,
                            MbrMaxX({self.geom_column}) maxX,
                            MbrMinY({self.geom_column}) minY,
                            MbrMaxY({self.geom_column}) maxY
                       FROM {self.name}
            """
            )
            # Add geometry column insert trigger
            conn.execute(
                f"""
                CREATE TRIGGER {rtree_table}_insert
               AFTER INSERT ON {self.name}
                          WHEN (new.{self.geom_column} NOT NULL
                           AND NOT ST_IsEmpty(NEW.{self.geom_column}))
                BEGIN
                  INSERT OR REPLACE INTO {rtree_table} VALUES (
                    NEW.{self.primary_key},
                    ST_MinX(NEW.{self.geom_column}),
                    ST_MaxX(NEW.{self.geom_column}),
                    ST_MinY(NEW.{self.geom_column}),
                    ST_MaxY(NEW.{self.geom_column})
                  );
                END;
            """
            )
            # Add geometry column (empty to non-empty) update trigger
            conn.execute(
                f"""
                CREATE TRIGGER {rtree_table}_update1
               AFTER UPDATE OF {self.geom_column} ON {self.name}
                          WHEN OLD.{self.primary_key} = NEW.{self.primary_key}
                           AND (
                               NEW.{self.geom_column} NOTNULL
                               AND NOT ST_IsEmpty(NEW.{self.geom_column})
                               )
                BEGIN
                  INSERT OR REPLACE INTO {rtree_table} VALUES (
                    NEW.{self.primary_key},
                    ST_MinX(NEW.{self.geom_column}),
                    ST_MaxX(NEW.{self.geom_column}),
                    ST_MinY(NEW.{self.geom_column}),
                    ST_MaxY(NEW.{self.geom_column})
                  );
                END;
            """
            )
            # Add geometry column (non-empty to empty) update trigger
            conn.execute(
                f"""
                CREATE TRIGGER {rtree_table}_update2
               AFTER UPDATE OF {self.geom_column} ON {self.name}
                          WHEN OLD.{self.primary_key} = NEW.{self.primary_key}
                           AND (
                                  NEW.{self.geom_column} ISNULL
                               OR ST_IsEmpty(NEW.{self.geom_column})
                               )
                BEGIN
                  DELETE FROM {rtree_table} WHERE id = OLD.{self.primary_key};
                END;
            """
            )
            # Add various column with non-empty geometry update trigger
            conn.execute(
                f"""
                CREATE TRIGGER {rtree_table}_update3 AFTER UPDATE ON {self.name}
                          WHEN OLD.{self.primary_key} != NEW.{self.primary_key}
                           AND (
                                   NEW.{self.geom_column} NOTNULL
                               AND NOT ST_IsEmpty(NEW.{self.geom_column})
                               )
                BEGIN
                  DELETE FROM {rtree_table} WHERE id = OLD.{self.primary_key};
                  INSERT OR REPLACE INTO {rtree_table} VALUES (
                    NEW.{self.primary_key},
                    ST_MinX(NEW.{self.geom_column}),
                    ST_MaxX(NEW.{self.geom_column}),
                    ST_MinY(NEW.{self.geom_column}),
                    ST_MaxY(NEW.{self.geom_column})
                  );
                END;
            """
            )
            # Add various column with empty geometry update trigger
            conn.execute(
                f"""
                CREATE TRIGGER {rtree_table}_update4 AFTER UPDATE ON {self.name}
                  WHEN OLD.{self.primary_key} != NEW.{self.primary_key}
                   AND (
                         NEW.{self.geom_column} ISNULL
                      OR ST_IsEmpty(NEW.{self.geom_column})
                       )
                BEGIN
                  DELETE FROM {rtree_table}
                        WHERE id IN (OLD.{self.primary_key}, NEW.{self.primary_key});
                END;
            """
            )
            # Add row deletion update trigger
            conn.execute(
                f"""
                CREATE TRIGGER {rtree_table}_delete AFTER DELETE ON {self.name}
                  WHEN old.{self.geom_column} NOT NULL
                BEGIN
                  DELETE FROM {rtree_table} WHERE id = OLD.{self.primary_key};
                END;
            """
            )

    def add_srs(self):
        # Add initial spatial ref
        proj = pyproj.crs.CRS.from_authority("epsg", self.srid)
        with self.gpkg.connect() as conn:
            conn.execute(
                """
                INSERT INTO gpkg_spatial_ref_sys
                            (
                                srs_name,
                                srs_id,
                                organization,
                                organization_coordsys_id,
                                definition
                            )
                     SELECT ?, ?, ?, ?, ?
                      WHERE NOT EXISTS(
                          SELECT 1 FROM gpkg_spatial_ref_sys WHERE srs_id = ?
                      )
            """,
                (
                    proj.name,
                    proj.to_epsg(),
                    proj.to_authority()[0],
                    proj.to_epsg(),
                    proj.to_wkt(),
                    proj.to_epsg(),
                ),
            )

    def drop_rtree(self):
        with self.gpkg.connect() as conn:
            # Drop rtree tables
            conn.execute(f"DROP TABLE rtree_{self.name}_{self.geom_column}")
            conn.execute(
                f"DROP TABLE rtree_{self.name}_{self.geom_column}_node"
            )
            conn.execute(
                f"DROP TABLE rtree_{self.name}_{self.geom_column}_rowid"
            )
            conn.execute(
                f"DROP TABLE rtree_{self.name}_{self.geom_column}_parent"
            )
            # Drop rtree indices
            conn.execut(
                f"DROP INDEX rtree_{self.name}_{self.geom_columns}_insert"
            )
            conn.execut(
                f"DROP INDEX rtree_{self.name}_{self.geom_columns}_update1"
            )
            conn.execut(
                f"DROP INDEX rtree_{self.name}_{self.geom_columns}_update2"
            )
            conn.execut(
                f"DROP INDEX rtree_{self.name}_{self.geom_columns}_update3"
            )
            conn.execut(
                f"DROP INDEX rtree_{self.name}_{self.geom_columns}_update4"
            )
            conn.execut(
                f"DROP INDEX rtree_{self.name}_{self.geom_columns}_delete"
            )

    def write_features(self, features, batch_size=10_000, counter=None):
        queue = []

        def write_queues():
            with self.gpkg.connect() as conn:
                template = self._sql_upsert_template
                n = len(queue)
                # TODO: look into performance of this strategy. Another option
                #       is to insert multiple values at once in a single
                #       statement.
                conn.executemany(template, queue)
            if counter is not None:
                counter.update(n)

        column_names = self._get_column_names()
        for feature in features:
            if len(queue) > batch_size:
                write_queues()
                queue = []

            new_columns = []
            for key, value in feature.items():
                if key == self.geom_column or key == self.primary_key:
                    continue
                if key not in column_names:
                    if value is None:
                        continue
                    new_columns.append((key, self._column_type(value)))

            if new_columns:
                if queue:
                    write_queues()
                queue = []
                self._add_feature_table_columns(new_columns)
                new_column_names = tuple(c[0] for c in new_columns)
                column_names = (*column_names, *new_column_names)

            values = tuple(self._row_value_generator(column_names, feature))

            queue.append(values)

        # Write stragglers
        write_queues()

    @property
    def _gp_header(self):
        version = self.gpkg.VERSION.to_bytes(1, byteorder="little")
        empty = self.gpkg.EMPTY.to_bytes(1, byteorder="little")
        srid = self.srid.to_bytes(4, byteorder="little")
        return b"GP" + version + empty + srid

    def _add_feature_table_columns(self, columns):
        with self.gpkg.connect() as conn:
            for column, value in columns:
                conn.execute(
                    f"ALTER TABLE {self.name} ADD COLUMN '{column}' {value}"
                )

    def _get_column_names(self):
        column_names = []
        with self.gpkg.connect() as conn:
            for table_info in conn.execute(f"PRAGMA table_info({self.name})"):
                column_name = table_info["name"]
                if column_name == self.primary_key:
                    continue
                column_names.append(column_name)
        return tuple(column_names)

    def _check_for_new_columns(self, old_column_names, ddict):
        keys = set(ddict.keys())
        new_column_names = keys.difference(old_column_names)
        new_columns = []
        for name in new_column_names:
            new_columns.append((name, self._column_type(ddict[name])))
        return new_columns

    def _add_new_columns(self, ddicts):
        column_names = self._get_column_names()
        columns_to_add = OrderedDict()
        for ddict in ddicts:
            for name, column_type in self._check_for_new_columns(
                column_names, ddict
            ):
                if name not in columns_to_add:
                    columns_to_add[name] = column_type
        self._add_feature_table_columns(list(columns_to_add.items()))

    def _column_type(self, value):
        column_type = COL_TYPE_MAP.get(type(value), None)
        if column_type is None:
            raise ValueError("Invalid column type")
        return column_type

    def _row_value_generator(self, column_names, d):
        for c in column_names:
            # Reserve primary key for internal use
            # TODO: raise warning(s) or rename source key to another column
            #       name if encountered.
            if c == self.primary_key:
                continue

            if self.geom_column in d:
                if c == self.geom_column:
                    yield self._serialize_geometry(d[self.geom_column])
                    continue
                if c == "_length":
                    yield haversine(d[self.geom_column]["coordinates"])
                    continue

            yield d.get(c, None)

    def _serialize_geometry(self, geometry):
        if isinstance(geometry, LineString) or isinstance(geometry, Point):
            return self._gp_header + geometry.wkb
        else:
            return self._gp_header + geomet.wkb.dumps(geometry)

    def _deserialize_geometry(self, geometry):
        # TODO: use geomet's built-in GPKG support?
        header_len = len(self._gp_header)
        wkb = geometry[header_len:]
        return geomet.wkb.loads(wkb)

    def serialize_row(self, row):
        row = {**row}
        if self.geom_column in row:
            row[self.geom_column] = self._serialize_geometry(
                row[self.geom_column]
            )
        return row

    def deserialize_row(self, row):
        # TODO: Implement this as a row handler for sqlite3 interface?
        return {
            **row,
            self.geom_column: self._deserialize_geometry(
                row[self.geom_column]
            ),
        }

    @property
    def _sql_upsert_template(self):
        """Generate an SQL template for upsert. Will work with or without column
        constraints.

        :returns: SQLite Template String
        :rtype: str
        """
        feature_table_columns = self._get_column_names()
        columns = ", ".join(feature_table_columns)
        placeholders = ", ".join("?" for c in feature_table_columns)
        sql = f"REPLACE INTO {self.name} ({columns}) VALUES ({placeholders})"
        return sql

    def __len__(self):
        with self.gpkg.connect() as conn:
            rows = conn.execute(f"SELECT COUNT() c FROM {self.name}")
            count = next(rows)["c"]
        return count

    def __iter__(self):
        sql = f"SELECT * FROM {self.name}"
        with self.gpkg.connect() as conn:
            for row in conn.execute(sql):
                yield self.deserialize_row(row)
