import contextlib
import os
import sqlite3
import tempfile

import geomet.wkb
import pyproj
from shapely.geometry import LineString, Point, shape
from shapely.ops import transform

from .utils import haversine


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

    def update(self, primary_key, ddict):
        columns = self._get_column_names()
        values = []

        keys = set(ddict.keys())
        new_columns = keys.difference(columns)
        if new_columns:
            cols_to_add = []
            for colname in new_columns:
                cols_to_add.append(
                    (colname, self._column_type(ddict[colname]))
                )
            self._add_feature_table_columns(cols_to_add)
            columns = self._get_column_names()

        keys = []

        for c in columns:
            if c in ddict:
                keys.append(c)
                values.append(ddict[c])

        set_clauses = ", ".join([f"{k} = ?" for k in keys])
        with self.gpkg.connect() as conn:
            conn.execute(
                f"""
                UPDATE {self.name}
                   SET {set_clauses}
                 WHERE {self.primary_key} = ?
            """,
                (*values, primary_key),
            )

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


class GeoPackage:
    VERSION = 0
    EMPTY = 1

    def __init__(self, path):
        self.path = path
        self._get_connection()
        self._setup_database()

        self.feature_tables = {}

        # Instantiate FeatureTables that already exist in the db
        with self.connect() as conn:
            table_rows = conn.execute(
                "SELECT table_name, srs_id FROM gpkg_contents"
            )
            table_rows = list(table_rows)

        for row in table_rows:
            table_name = row["table_name"]

            with self.connect() as conn:
                geom_type_query = conn.execute(
                    """
                    SELECT geometry_type_name
                      FROM gpkg_geometry_columns
                     WHERE table_name = ?
                """,
                    (table_name,),
                )
                geom_type = next(geom_type_query)["geometry_type_name"]

            self.feature_tables[table_name] = FeatureTable(
                self, table_name, geom_type, srid=row["srs_id"]
            )

    def add_feature_table(self, name, geom_type, srid=4326):
        table = FeatureTable(self, name, geom_type, srid=srid)
        table.create_tables()
        self.feature_tables[name] = table
        return table

    def drop_feature_table(self, name):
        table = self.feature_tables.pop(name)
        table.drop_tables()

    def _get_connection(self):
        conn = sqlite3.connect(self.path, uri=True)
        conn.enable_load_extension(True)
        # Spatialite used for rtree-based functions (MinX, etc). Can eventually
        # replace or make configurable with other extensions.
        conn.load_extension("mod_spatialite.so")
        conn.row_factory = self._dict_factory
        self.conn = conn

    @contextlib.contextmanager
    def connect(self):
        # FIXME: monitor connection and ensure that it is good. Handle
        #        in-memory case.
        yield self.conn
        self.conn.commit()
        # FIXME: downsides of not calling conn.close? It's necessary to note
        #        call conn.close for in-memory databases. May want to change
        #        this behavior depending on whether the db is on-disk or
        #        in-memory.

    def _setup_database(self):
        if self.path is None:
            # TODO: revisit this behavior. Creating a temporary file by default
            #       may be undesirable.
            # Create a temporary path, get the name
            _, path = tempfile.mkstemp(suffix=".gpkg")
            self.path = str(path)
            # Delete the path to prepare for fresh db
            os.remove(path)

        if self._is_empty_database():
            self._create_database()

    def _is_empty_database(self):
        with self.connect() as conn:
            query = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
            try:
                next(query)
                return False
            except StopIteration:
                return True

    def _create_database(self):
        with self.connect() as conn:
            # Set the format metadata
            conn.execute(f"PRAGMA application_id = {GPKG_APPLICATION_ID}")
            conn.execute(f"PRAGMA user_version = {GPKG_USER_VERSION}")

            # Create gpkg_contents table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gpkg_contents (
                    table_name TEXT,
                    data_type TEXT NOT NULL,
                    identifier TEXT UNIQUE,
                    description TEXT DEFAULT '',
                    last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    min_x DOUBLE,
                    min_y DOUBLE,
                    max_x DOUBLE,
                    max_y DOUBLE,
                    srs_id INTEGER,
                    PRIMARY KEY (table_name)
                )
            """
            )

            # Create gpkg_extensions table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gpkg_extensions(
                    table_name TEXT,
                    column_name TEXT,
                    extension_name TEXT NOT NULL,
                    definition TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    UNIQUE (table_name, column_name, extension_name)
                )
            """
            )

            # Create gpkg_geometry_columns table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gpkg_geometry_columns(
                    table_name TEXT UNIQUE NOT NULL,
                    column_name TEXT NOT NULL,
                    geometry_type_name TEXT NOT NULL,
                    srs_id INTEGER NOT NULL,
                    z TINYINT NOT NULL,
                    m TINYINT NOT NULL,
                    PRIMARY KEY (table_name, column_name)
                )
            """
            )

            # Create gpkg_ogr_contents table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gpkg_ogr_contents(
                    table_name TEXT NOT NULL,
                    feature_count INTEGER DEFAULT NULL,
                    PRIMARY KEY (table_name)
                )
            """
            )

            # Create gpkg_spatial_ref_sys
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys(
                    srs_name TEXT NOT NULL,
                    srs_id INTEGER NOT NULL,
                    organization TEXT NOT NULL,
                    organization_coordsys_id INTEGER NOT NULL,
                    definition TEXT NOT NULL,
                    description TEXT,
                    PRIMARY KEY (srs_id)
                )
            """
            )

    def copy(self, path):
        """Copies the current GeoPackage to a new location and returns a new instance
        of a GeoPackage. A convenient way to create an in-memory GeoPackage, as
        path can be any SQLite-compatible connection string, including
        :memory:.

        :param path: Path to the new database. Any SQLite connection string can
                     be used.
        :type path: str

        """
        # TODO: catch the "memory" string and ensure that it includes a name
        #       and shared cache. Our strategy requires reconnecting to the db,
        #       so it must persist in memory.

        new_conn = sqlite3.connect(path)
        new_conn.enable_load_extension(True)
        # Spatialite used for rtree-based functions (MinX, etc). Can eventually
        # replace or make configurable with other extensions.
        new_conn.load_extension("mod_spatialite.so")

        with self.connect() as conn:
            # Set row_factory to none for iterdumping
            conn.row_factory = None

            # Copy over all tables but not indices
            for line in conn.iterdump():
                # Skip all index creation - these should be recreated
                # afterwards
                if "CREATE TABLE" in line or "INSERT INTO" in line:
                    # TODO: derive index names from metadata table instead
                    if "idx_" in line:
                        continue
                    if "rtree_" in line:
                        continue
                if "COMMIT" in line:
                    continue
                new_conn.cursor().executescript(line)

            # Copy over all indices
            for line in conn.iterdump():
                # Recreate the indices
                if "CREATE TABLE" in line or "INSERT INTO" in line:
                    if "idx_" in line:
                        new_conn.cursor().executescript(line)
                if "COMMIT" in line:
                    continue

            # TODO: rtree strategy is different? Why?
            # for line in conn.iterdump():
            #     # Recreate the indices
            #     if "CREATE TABLE" in line or "INSERT INTO" in line:
            #         if "rtree_" in line:
            #             new_conn.cursor().executescript(line)
            #     if "COMMIT" in line:
            #         continue
            conn.row_factory = self._dict_factory

        new_db = GeoPackage(path)

        return new_db

    @staticmethod
    def _dict_factory(cursor, row):
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
