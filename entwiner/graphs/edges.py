"""Reusable, sqlite-backed edge containers"""


class ReadOnlyEdge:
    """Retrieves edge attributes from table, does not allow assignment.

    """

    def __init__(self, sqlitegraph=None, _u=None, _v=None):
        self.sqlitegraph = sqlitegraph
        self.u = _u
        self.v = _v
        self.delayed_attr = {}

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def keys(self):
        return self.sqlitegraph.get_edge_attr(self.u, self.v).keys()

    def items(self):
        return self.sqlitegraph.get_edge_attr(self.u, self.v).items()

    def update(self, attr):
        if self.u is not None and self.v is not None:
            self.sqlitegraph.update_edge(self.u, self.v, attr)
        else:
            self.delayed_attr.update(attr)

    def __getitem__(self, key):
        return self.sqlitegraph.get_edge_attr(self.u, self.v)[key]

    def __setitem__(self, key, value):
        self.update({key: value})

    def __bool__(self):
        if self.sqlitegraph.get_edge_attr(self.u, self.v):
            return True
        else:
            return False

    def __iter__(self):
        return iter(self.sqlitegraph.get_edge_attr(self.u, self.v))


class RealizedEdge:
    """Edge that stores data in a dict, can be initialized in a dict, and syncs to DB.

    :param sqlitegraph: SQLiteGraph instance, required for talking to sqlite db.
    :type sqlitegraph: entwiner.SQLiteGraph
    :param _u: First node of the edge.
    :type _u: str
    :param _v: Second node of the edge.
    :type v: str

    """

    def __init__(self, sqlitegraph=None, _u=None, _v=None, **kwargs):
        self.sqlitegraph = sqlitegraph
        self.u = _u
        self.v = _v
        self.dict = dict(**kwargs)

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def keys(self):
        return self.dict.keys()

    def items(self):
        return self.dict.items()

    def __getitem__(self, key):
        return self.dict[key]

    def __bool__(self):
        return bool(self.dict)

    def __iter__(self):
        return iter(self.dict)


# FIXME: inherit from MutableMapping abc, might fix various dict compatibility issues
class Edge(ReadOnlyEdge):
    """Retrieves edge attributes from table, allows direct assignment of values as a
    dict-like.

    :param sqlitegraph: SQLiteGraph instance, required for talking to sqlite db.
    :type sqlitegraph: entwiner.SQLiteGraph
    :param _u: First node of the edge.
    :type _u: str
    :param _v: Second node of the edge.
    :type v: str

    """

    def update(self, attr):
        if self.u is not None and self.v is not None:
            self.sqlitegraph.update_edge(self.u, self.v, attr)
        else:
            self.delayed_attr.update(attr)

    def __setitem__(self, key, value):
        self.update({key: value})
