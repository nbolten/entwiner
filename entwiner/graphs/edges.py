"""Reusable, sqlite-backed edge containers"""
from collections.abc import Mapping, MutableMapping
from functools import partial

from entwiner.exceptions import ImmutableGraphError, UninitializedEdgeError


class EdgeDict(MutableMapping):
    """A mutable mapping that always syncs to/from the database edges table."""

    def __init__(self, _sqlitegraph=None, _u=None, _v=None):
        self.sqlitegraph = _sqlitegraph
        self.u = _u
        self.v = _v

    def __getitem__(self, key):
        return self.sqlitegraph.get_edge_attr(self.u, self.v)[key]

    def __iter__(self):
        # TODO: speed up by directly asking for keys for this row?
        return iter(self.sqlitegraph.get_edge_attr(self.u, self.v))

    def __len__(self):
        return len(self.keys())

    def __setitem__(self, key, value):
        if self.u is not None and self.v is not None:
            self.sqlitegraph.set_edge_attr(self.u, self.v, key, value)
        else:
            raise UninitializedEdgeError("Attempt to set attrs on uninitialized edge.")

    def __delitem__(self, key):
        self.sqlitegraph.set_edge_attr(self.u, self.v, key, None)


class EdgeView(Mapping):
    """Read-only edge attributes that can be updated from the SQLite database or
    initialized with kwargs (kwargs will be stored in-memory).

    :param _sqlitegraph: SQLiteGraph used for interacting with underlying graph db.
    :type _sqlitegraph: SQLiteGraph
    :param _u: first node describing (u, v) edge.
    :type _u: str
    :param _v: second node describing (u, v) edge.
    :type _v: str
    :param kwargs: Dict-like data.
    :type kwargs: dict-like data as keyword arguments.

    """

    ddict_factory = dict

    def __init__(self, _sqlitegraph=None, _u=None, _v=None, **kwargs):
        self.sqlitegraph = _sqlitegraph
        self.u = _u
        self.v = _v
        self.ddict_factory = self.ddict_factory
        self.ddict = self.ddict_factory()
        self.ddict.update(kwargs)

    def __getitem__(self, key):
        return self.ddict[key]

    def __iter__(self):
        return iter(self.ddict)

    def __len__(self):
        return len(self.ddict)

    def sync_from_db(self):
        self.ddict = dict(self.sqlitegraph.get_edge_attr(u, v))

    def sync_to_db(self):
        raise ImmutableGraphError(
            "Attempt to write edge attributes to immutable graph."
        )

    @classmethod
    def from_db(cls, sqlitegraph, u, v):
        return cls(
            _sqlitegraph=sqlitegraph, _u=u, _v=v, **self.sqlitegraph.get_edge_attr(u, v)
        )


class Edge(EdgeView, MutableMapping):
    """Edge attributes that can be updated from the SQLite database or initialized with
    kwargs (kwargs will be stored in-memory).

    :param _sqlitegraph: SQLiteGraph used for interacting with underlying graph db.
    :type _sqlitegraph: SQLiteGraph
    :param _u: first node describing (u, v) edge.
    :type _u: str
    :param _v: second node describing (u, v) edge.
    :type _v: str
    :param kwargs: Dict-like data.
    :type kwargs: dict-like data as keyword arguments.

    """

    ddict_factory = EdgeDict

    def __init__(self, *args, _sqlitegraph=None, **kwargs):
        self.ddict = partial(self.ddict_factory, _sqlitegraph)
        super().__init__(*args, _sqlitegraph=_sqlitegraph, **kwargs)

    def __setitem__(self, key, value):
        if self.u is not None and self.v is not None:
            self.sqlitegraph.set_edge_attr(self.u, self.v, key, value)
        raise UninitializedEdgeError("Attempt to set attrs on uninitialized edge.")

    def __delitem__(self, key):
        self.sqlitegraph.set_edge_attr(self.u, self.v, key, None)

    def sync_to_db(self):
        self.sqlitegraph.insert_or_replace_edge(self.u, self.v, self.ddict)
