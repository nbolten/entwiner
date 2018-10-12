"""Database management functions."""
from heapq import heappop, heappush
from itertools import count
import os
import sqlite3

from . import costs


PRECISION = 6


class EdgeDB:
    """Container for managing features as edges in an sqlite3 database.

    :param database: An sqlite3-compatible database string: a file path or :memory:.
    :type database: str

    """
    def __init__(self, database):
        self.database = database
        # TODO: most models acquire a connection with every call - consider this
        self.conn = sqlite3.connect(self.database)

    def stage(self):
        """Creates a temporary database if one doesn't already exist."""
        # Delete temporary db if it exists.
        if os.path.exists(self.database):
            os.remove(self.database)

        if self.database != ":memory:":
            self.database += ".tmp"

        self.conn = sqlite3.connect(self.database)

        # TODO: enable Spatialite, add geometry column in initial step
        self.conn.execute("CREATE TABLE edges (u integer, v integer)")
        self.conn.execute("CREATE TABLE nodes (x real, y real, UNIQUE(x, y))")

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

        def get_or_fetch_node(feature, index):
            coords = feature["geometry"]["coordinates"][index]
            x, y = [round(c, PRECISION) for c in coords]
            rows = cursor.execute("SELECT rowid FROM nodes WHERE x = ? AND y = ?", (x, y))
            row = rows.fetchone()
            if not row:
                cursor.execute("INSERT INTO nodes VALUES (?, ?)", (x, y))
                return cursor.execute("SELECT rowid FROM nodes WHERE x = ? AND y = ?", (x, y)).fetchone()[0]
            else:
                return row[0]

        # Update the table
        inserts = []
        for feature in features:
            values = [feature["properties"].get(c, None) for c in cols]
            u = get_or_fetch_node(feature, 0)
            v = get_or_fetch_node(feature, -1)
            values[0] = u
            values[1] = v
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

    def shortest_path(self, sources, cost_fun=costs.shortest_path, cutoff=None, target=None):
        """
        :param sources: list of originating node ids.
        :type sources: list of ints
        :param cost_fun: Cost function taking (u, v, d) positional arguments and
                         returning a number.
        :type cost_fun: callable
        :param cutoff: Cumulative cost at which to stop searching. If None, search is
                       exhaustive.
        :type cutoff: float
        :param target: A single destination node id.
        :type target: int

        """
        # Dijkstra's algorithm. TODO: make swappable shortest path implementations
        paths = {}
        pred = {}
        for source in sources:
            paths[source] = [source]
            pred[source] = []

        # Retrieve database column names
        cursor = self.conn.cursor()
        ignore = ["id", "geometry", "u"]
        colnames = [c[1] for c in cursor.execute("PRAGMA table_info(edges)") if c not in ignore]

        def adj_nodes(u):
            # The first part of the result is the node ID, rest is edge attr
            neighbors = []

            # TODO: the SQL queries take up ~40% of runtime for large-ish traversals (whole
            # graph of 75k pahs), with 50% of that taken up by the execute_sql line and 25%
            # taken up by iterating over the cursor. Two ideas for speeding this up:
            # 1) Find a way to more rapidly get all of the rows without the iterator - we just
            #    want all of it at once. fetchall() does not make anything faster.
            # 2) The check for whether a node has already been visited happens after the SQL
            #    query and after coercing to dict. Any reasonable way to restrict query to
            #    only 'unseen' nodes (note that 'seen' list grows massively, has heavy i/o
            #    burden).
            colreplace = ', '.join(colnames)
            query = 'SELECT {} FROM edges WHERE u = ?'.format(colreplace)
            cursor = self.conn.execute(query, (u,))

            for row in cursor:
                data = {}
                for colname, value in zip(colnames, row):
                    if value is not None:
                        data[colname] = value
                v = int(data.pop('v'))
                neighbors.append((v, data))

            return neighbors

        # FIXME: the following used to be wrapped in a db.atomic() context. Find
        # sqlite3 raw equivalent

        push = heappush
        pop = heappop
        dist = {}  # dictionary of final distances
        seen = {}
        # fringe is heapq with 3-tuples (distance,c,node)
        # use the count c to avoid comparing nodes (may not be able to)
        c = count()
        fringe = []
        for source in sources:
            seen[source] = 0
            push(fringe, (0, next(c), source))
        while fringe:
            (d, _, v) = pop(fringe)
            if v in dist:
                continue  # already searched this node.
            dist[v] = d
            if v == target:
                break
            for u, e in adj_nodes(v):
                cost = cost_fun(v, u, e)
                if cost is None:
                    continue
                # NOTE: dist[v] could be precalculated if it's a bottleneck
                vu_dist = dist[v] + cost
                if cutoff is not None:
                    if vu_dist > cutoff:
                        continue
                if u in dist:
                    if vu_dist < dist[u]:
                        raise ValueError('Contradictory paths found:',
                                         'negative weights?')
                elif u not in seen or vu_dist < seen[u]:
                    seen[u] = vu_dist
                    push(fringe, (vu_dist, next(c), u))
                    if paths is not None:
                        paths[u] = paths[v] + [u]
                    if pred is not None:
                        pred[u] = [v]
                elif vu_dist == seen[u]:
                    if pred is not None:
                        pred[u].append(v)

        if target is None:
            return (dist, paths)
        try:
            return (dist[target], paths[target])
        except KeyError:
            # TODO: create a custom no path exception class
            raise NoPath('No path to {}'.format(target))

        return dist

    def _sqlite_type(self, value):
        if type(value) == int:
            return "integer"
        elif type(value) == float:
            return "real"
        else:
            return "text"
