"""Dict-like interface(s) for graphs."""
import sqlite3

from .utils import sqlite_type


# edge_attr_dict_factory: stores edge attrs, will be serialized to db by other data
# structures, is effectively ephemeral (hopefully) outside of attempts to iterate over
# the db edge attrs, in which case the whole db will be put in memory (one row at a
# time) anyways.
edge_attr_db_factory = dict

# adjlist_inner_dict_factory:
def adjlist_inner_db_factory_factory(database):
    # TODO: investigate the need for a pool and/or method for attempting to recreate
    # the connection, i.e instead of self.conn use self.get_connection()
    conn = sqlite3.connect(database)

    def columns(self):
        cursor = self.conn.cursor()
        return [c[1] for c in cursor.execute("PRAGMA table_info(edges)")]

    def getitem(self, key):
        cursor = self.conn.cursor()
        # NOTE: note that this relies on the _u property being set on the object, this
        # is controlled entirely by the parent creating this class: it needs to be
        # added during __setitem__ in the adjlist_outer_dict_factory.
        u = self._u
        v = key
        query = cursor.execute("SELECT * FROM edges WHERE u = ? AND v = ", (u, v))
        columns = self.columns()  # TODO: memoize and/or store as attr, not method
        data = {k: v for row in query for k, v in zip(columns, row) if v is not None}
        return data

    def setitem(self, key, value):
        # Item must be dict-like and 'flat'
        cursor = self.conn.cursor()
        columns = self.columns()
        u = self._u
        v = key
        query = cursor.execute("SELECT * FROM edges WHERE u = ? AND v = ?", (u, v))
        if query.fetchone():
            # There's already data: we need to do an update.
            # FIXME: this will almost definitely need to be reorganized for a
            # MultiDiGraph, as it will return / operate on another layer of dict-likes
            # that are keyed to incrementing integers by default. In other words,
            # G[u][v] would return a dict-like with keys like 0, 1, etc each referring
            # to their own edge data.
            # FIXME: this will need to vary between Graph and DiGraph - update just
            # u, v vs. update u, v and v, u.
            # See if we need to create new columns
            # Value should be dict-like. TODO: add check?
            sql_set = []
            for k, v in value.items():
                if k not in columns:
                    col_type = sqlite_type(feature['properties'][key])
                    cursor.execute("ALTER TABLE edges ADD COLUMN {} {}".format(key, col_type))
                    columns.append(k)
                    sql_set.append("{}={}".format(k, v))

            template = """
                UPDATE edges
                   SET {}
                 WHERE u = ?
                   AND v = ?
            """.format(", ".format(sql_set))

            template.format(["=".join((k, v) for k, v in value.items()
            cursor.execute("UPDATE edges SET ")
        else:
            # There is not already data: do an insert
