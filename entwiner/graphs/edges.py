"""Reusable, sqlite-backed edge containers"""
from collections.abc import Mapping, MutableMapping
from functools import partial

from entwiner.exceptions import ImmutableGraphError, UninitializedEdgeError


class EdgeDict(MutableMapping):
    """A mutable mapping that always syncs to/from the database edges table."""

    def __init__(self, _network=None, _u=None, _v=None):
        self.network = _network
        self.u = _u
        self.v = _v

    def __getitem__(self, key):
        return self.network.edges.get_edge(self.u, self.v)[key]

    def __iter__(self):
        # TODO: speed up by directly asking for keys for this row?
        return iter(self.network.edges.get_edge(self.u, self.v))

    def __len__(self):
        return len(self.keys())

    def __setitem__(self, key, value):
        if self.u is not None and self.v is not None:
            self.network.edges.update(self.u, self.v, {key: value})
        else:
            raise UninitializedEdgeError(
                "Attempted to set attrs on uninitialized edge."
            )

    def __delitem__(self, key):
        if self.u is not None and self.v is not None:
            self.network.edges.update(self.u, self.v, {key: None})
        else:
            raise UninitializedEdgeError(
                "Attempted to delete attrs on uninitialized edge."
            )


class EdgeView(Mapping):
    """Read-only edge attributes that can be updated from the GeoPackage
    (SQLite) database or initialized with kwargs (kwargs will be stored
    in-memory).

    :param _network: GeoPackageNetwork used for interacting with underlying graph db.
    :type _network: entwiner.GeoPackageNetwork
    :param _u: first node describing (u, v) edge.
    :type _u: str
    :param _v: second node describing (u, v) edge.
    :type _v: str
    :param kwargs: Dict-like data.
    :type kwargs: dict-like data as keyword arguments.

    """

    def __init__(self, _network=None, _u=None, _v=None, **kwargs):
        self.network = _network
        self.u = _u
        self.v = _v
        self.ddict = dict()
        if kwargs:
            self.ddict.update(kwargs)
        else:
            self.sync_from_db()

    def __getitem__(self, key):
        return self.ddict[key]

    def __iter__(self):
        return iter(self.ddict)

    def __len__(self):
        return len(self.ddict)

    def sync_from_db(self):
        self.ddict = dict(self.network.edges.get_edge(self.u, self.v))

    def sync_to_db(self):
        raise ImmutableGraphError(
            "Attempt to write edge attributes to immutable graph."
        )

    @classmethod
    def from_db(cls, network, u, v):
        return cls(
            _network=network, _u=u, _v=v, **self.network.edges.get_edge(self.u, self.v)
        )


class Edge(EdgeView, MutableMapping):
    """Edge attributes that can be updated from the SQLite database or initialized with
    kwargs (kwargs will be stored in-memory).

    :param _network: GeoPackageNetwork used for interacting with underlying graph db.
    :type _network: entwiner.GeoPackageNetwork
    :param _u: first node describing (u, v) edge.
    :type _u: str
    :param _v: second node describing (u, v) edge.
    :type _v: str
    :param kwargs: Dict-like data.
    :type kwargs: dict-like data as keyword arguments.

    """

    def __init__(self, *args, _network=None, _u=None, _v=None, **kwargs):
        self.network = _network
        self.u = _u
        self.v = _v
        self.ddict = EdgeDict(_network=_network, _u=_u, _v=_v)
        if kwargs:
            self.ddict.update(kwargs)

    # def __iter__(self):
    #     self.sync_from_db()
    #     return super().__iter__()

    def __setitem__(self, key, value):
        self.ddict[key] = value

    def __delitem__(self, key):
        del self.ddict[key]

    def sync_to_db(self):
        self.network.insert_or_replace_edge(self.u, self.v, self.ddict)
