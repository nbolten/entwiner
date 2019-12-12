"""Reusable, sqlite-backed edge containers"""
from collections.abc import Mapping, MutableMapping

from entwiner.exceptions import ImmutableGraphError


class ImmutableEdge(Mapping):
    """Retrieves edge attributes from table, but does not allow assignment.

    :param _sqlitegraph: The SQLite-backed graph class.
    :type _sqlitegraph: entwiner.SqliteGraph
    :param _u: Incoming node.
    :type _u: str
    :param _v: Outgoing node.
    :type _v: str

    """

    def __init__(self, _sqlitegraph=None, _u=None, _v=None):
        self.sqlitegraph = _sqlitegraph
        self.u = _u
        self.v = _v

    def __getitem__(self, key):
        return self.sqlitegraph.get_edge_attr(self.u, self.v)[key]

    def __iter__(self):
        return iter(self.sqlitegraph.get_edge_attr(self.u, self.v))

    def __len__(self):
        return len(self.keys())


class Edge(MutableMapping):
    """Retrieves edge attributes from table, allows direct assignment of values as a
    dict-like.

    :param sqlitegraph: SQLiteGraph instance, required for talking to sqlite db.
    :type sqlitegraph: entwiner.SQLiteGraph
    :param _u: First node of the edge.
    :type _u: str
    :param _v: Second node of the edge.
    :type v: str

    """

    def __init__(self, _sqlitegraph=None, _u=None, _v=None):
        self.sqlitegraph = sqlitegraph
        self.u = _u
        self.v = _v

    def __getitem__(self, key):
        return self.sqlitegraph.get_edge_attr(self.u, self.v)[key]

    def __iter__(self):
        return iter(self.sqlitegraph.get_edge_attr(self.u, self.v))

    def __len__(self):
        return len(self.keys())

    def __setitem__(self, key, value):
        if self.u is not None and self.v is not None:
            self.sqlitegraph.set_edge_attr(self.u, self.v, key, value)
        raise UninitializedEdgeError("Attempt to set attrs on uninitialized edge.")

    def __delitem__(self, key):
        self.sqlitegraph.set_edge_attr(self.u, self.v, key, None)
