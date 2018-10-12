"""Database management functions."""
import os
import sqlite3


PRECISION = 6


class EdgeDB:
    """Container for managing features as edges in an sqlite3 database.

    :param database: An sqlite3-compatible database string: a file path or :memory:.
    :type database: str

    """
    def __init__(self, database):
        self.database = database
        self._initialize()

    def _initialize(self):
        """Creates a database if one doesn't already exist."""
        if self.database != ":memory:":
            self.database += ".tmp"

        # Delete temporary db if it exists.
        if os.path.exists(self.database):
            os.remove(self.database)

        self.conn = sqlite3.connect(self.database)

        # TODO: enable Spatialite, add geometry column in initial step
        self.conn.execute("CREATE TABLE edges (u varchar, v varchar)")

    def add_edges(self, features):
        """Inserts a batch of features into the edges table.

        :param features: a list of LineString features in GeoJSON format.
        :type features: list of GeoJSON-like dicts

        """
        # See if we need to create any new columns based on the input data.
        cursor = self.conn.cursor()
        cols = [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]
        cols_set = set(cols)

        for feature in features:
            for key in feature['properties'].keys():
                if key not in cols_set:
                    col_type = self._sqlite_type(feature['properties'][key])
                    cursor.execute("ALTER TABLE edges ADD COLUMN {} {}".format(key, col_type))
                    cols.append(key)
                    cols_set.add(key)

        # Update the table
        inserts = []
        for feature in features:
            values = [feature["properties"].get(c, None) for c in cols]
            u = [round(c, PRECISION) for c in feature["geometry"]["coordinates"][0]]
            v = [round(c, PRECISION) for c in feature["geometry"]["coordinates"][-1]]
            u_str = str(tuple(u))
            v_str = str(tuple(v))
            values[0] = u_str
            values[1] = v_str
            inserts.append(values)

        paramstring = ", ".join("?" for i in range(len(cols)))
        template = "INSERT INTO edges VALUES ({})".format(paramstring)
        cursor.executemany(template, inserts)
        self.conn.commit()

    def finalize(self):
        if self.database.endswith(".db.tmp"):
            new_database = self.database[:-4]
            os.rename(self.database, new_database)
            self.database = new_database

    def _sqlite_type(self, value):
        if type(value) == int:
            return "integer"
        elif type(value) == float:
            return "real"
        else:
            return "text"
