"""Reusable, sqlite-backed edge containers"""


# FIXME: inherit from MutableMapping abc, might fix various dict compatibility issues
class Edge:
    """Retrieves edge attributes from table, allows direct assignment of values as a
    dict-like.

    """

    def __init__(self, _graphdb=None, _u=None, _v=None):
        self.graphdb = _graphdb
        self.u = _u
        self.v = _v
        self.delayed_attr = {}

    def get(self, key, defaults):
        try:
            return self[key]
        except KeyError:
            return defaults

    def keys(self):
        return self.graphdb.get_edge_attr(self.u, self.v).keys()

    def items(self):
        return self.graphdb.get_edge_attr(self.u, self.v).items()

    def update(self, attr):
        if self.u is not None and self.v is not None:
            self.graphdb.update_edge(self.u, self.v, attr)
        else:
            self.delayed_attr.update(attr)

    def __getitem__(self, key):
        return self.graphdb.get_edge_attr(self.u, self.v)[key]

    def __setitem__(self, key, value):
        self.update({key: value})

    def __bool__(self):
        if self.graphdb.get_edge_attr(self.u, self.v):
            return True
        else:
            return False

    def __iter__(self):
        return iter(self.graphdb.get_edge_attr(self.u, self.v))


class RealizedEdge:
    """Edge that stores data in a dict, can be initialized in a dict, and syncs to DB.

    """

    def __init__(self, _graphdb=None, _u=None, _v=None, **kwargs):
        self.graphdb = _graphdb
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

    def update(self, attr):
        self.dict.update(attr)
        self.graphdb.update_edge(self.u, self.v, attr)

    def __getitem__(self, key):
        return self.dict[key]

    def __setitem__(self, key, value):
        self.dict[key] = value
        self.graphdb.update_edge(self.u, self.v, attr)

    def __bool__(self):
        return bool(self.dict)

    def __iter__(self):
        return iter(self.dict)
