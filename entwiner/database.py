"""Database management functions."""
import os
import sqlite3


PRECISION = 6


# TODO: consider making a class - share db connection
def initialize_db(path_or_pattern):
    if path_or_pattern != ":memory:":
        path_or_pattern += ".tmp"

    # Delete temporary db if it exists.
    if os.path.exists(path_or_pattern):
        os.remove(path_or_pattern)

    db = sqlite3.connect(path_or_pattern)

    # TODO: enable Spatialite, add geometry column in initial step

    db.execute("CREATE TABLE edges (u varchar, v varchar)")

    return db


def add_edge(db, feature):
    # Feature will have geometry in 'geometry' key and attributes in 'properties' key
    # See if it has any new columns not currently in edges
    c = db.cursor()
    cols = [c[1] for c in c.execute("PRAGMA table_info(edges)")]

    props = feature['properties']
    missing = set(props.keys()) - set(cols)
    if missing:
        for key in missing:
            # Guess data type from first entry
            col_type = sqlite_type(props[key])
            # FIXME: don't use string subs. For some reason initial escaped sub attempt didn't work
            # c.execute("ALTER TABLE edges ADD COLUMN ? ?", (key, col_type))
            c.execute("ALTER TABLE edges ADD COLUMN {} {}".format(key, col_type))
            cols.append(key)

    # Update the table
    values = [props.get(c, None) for c in cols]
    u = str(tuple([round(c, PRECISION) for c in feature["geometry"]["coordinates"][0]]))
    v = str(tuple([round(c, PRECISION) for c in feature["geometry"]["coordinates"][0]]))
    subs = ", ".join("?" for v in values)
    c.execute("INSERT INTO edges VALUES ({})".format(subs), values)
    db.commit()


def finalize_db(path):
    # Just rename it
    # FIXME: this doesn't seem to be happening
    if path.endswith(".db.tmp"):
        os.rename(path, path[:-4])


def sqlite_type(value):
    if type(value) == int:
        return "integer"
    elif type(value) == float:
        return "float"
    else:
        return "varchar"
