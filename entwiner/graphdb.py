import sqlite3


# TODO: generalize to non-sqlite3 DBs
class GraphDB:
    def __init__(self, path):
        self.path = path
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.enable_load_extension(True)
        conn.load_extension("mod_spatialite.so")
        self.conn = conn

    def add_cols_if_not_exist_sql(self, colnames, values, table):
        existing_cols = set(
            [c[1] for c in self.conn.execute("PRAGMA table_info({})".format(table))]
        )
        sql_list = []
        for colname, value in zip(colnames, values):
            if colname not in existing_cols:
                col_type = sqlite_type(ddict[key])
                sql_list.append(
                    "ALTER TABLE {} ADD COLUMN {} {};".format(table, colname, col_type)
                )

        return "\n".join(sql_list)

    def add_cols_if_not_exist(self, keys, values, table):
        sql = self.add_cols_if_not_exist_sql(keys, values, table)
        self.conn.execute(sql)
        self.conn.commit()

    def get_node(self, key):
        query = self.conn.execute(
            "SELECT AsGeoJSON(_geometry) _geometry, * FROM nodes WHERE _key = ?", (key,)
        )
        data = dict(query.fetchone())
        if data is None:
            raise NodeNotFoundError("Specified node does not exist.")
        data.pop("_key")
        return data

    def add_node(self, key, ddict=None):
        if ddict is None:
            ddict = {}

        keys, values = zip(*ddict.items())
        self.add_cols_if_not_exist(keys, values, "nodes")

        template = "INSERT INTO nodes ({}) VALUES ({})"
        col_str = ", ".join(["_key"] + keys)
        val_str = ", ".join(["?" for v in values])
        sql = template.format(col_str, val_str)
        self.conn.execute(sql, [str(key)] + values)
        self.conn.commit()

    def update_node(self, key, ddict):
        if not ddict:
            return

        keys, values = zip(*ddict.items())
        self.add_cols_if_not_exist(keys, values, "nodes")

        template = "UPDATE nodes SET {} WHERE _key = ?"
        assignments = ["{} = ?".format(k) for k in keys]
        sql = template.format(", ".join(assignments))
        self.conn.execute(sql, (values + [key],))
        self.conn.commit()

    def get_edge_attr(self, u, v):
        query = self.conn.execute(
            "SELECT AsGeoJSON(_geometry) _geometry, * FROM edges WHERE _u = ? AND _v = ?",
            (u, v),
        )
        row = query.fetchone()
        if row is None:
            raise EdgeNotFoundError("No such edge exists.")

        data = dict(row)
        data.pop("_u")
        data.pop("_v")
        return {key: value for key, value in data.items() if value is not None}

    def add_edge_sql(self, _u, _v, ddict):
        keys, values = zip(*ddict.items())
        add_cols_sql = self.add_cols_if_not_exist_sql(keys, values, "edges")

        template = "INSERT OR REPLACE INTO edges ({}) VALUES ({});"
        cols_str = ", ".join(["_u", "_v"] + keys)
        values_str = ", ".join(["?" for v in range(len([_u, _v]) + len(values))])
        template = template.format(cols_str, values_str)
        sql = add_cols_sql + "\n" + template
        return sql

    def add_edge(self, _u, _v, ddict):
        sql = self.add_edge_sql(_u, _v, ddict)
        self.conn.executescript(sql)
        self.conn.commit()

    def update_edge(self, u, v, ddict):
        if ddict:
            keys, values = zip(*ddict.items())
            col_sql = self.add_cols_if_not_exist_sql(keys, values, "edges")

            template = "UPDATE edges SET {} WHERE _u = ? AND _v = ?;"
            assignments = ["{} = ?".format(k) for k in keys]
            sql = template.format(", ".join(assignments))

            self.conn.execute(col_sql)
            self.conn.execute(sql, list(values) + [u, v])
            self.conn.commit()
