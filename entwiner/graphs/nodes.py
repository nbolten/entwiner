"""Reusable sqlite-backed Node container(s)."""
from collections.abc import Mapping, MutableMapping

import networkx as nx

from ..exceptions import NodeNotFound


class NodesView(Mapping):
    """An immutable mapping from node IDs to nodes. Used by NetworkX classes to iterate
    over nodes.

    :param _sqlitegraph: The SQLite-backed graph class.
    :type _sqlitegraph: entwiner.SqliteGraph

    """

    def __init__(self, _sqlitegraph=None, *args, **kwargs):
        self.sqlitegraph = _sqlitegraph

    def __getitem__(self, key):
        return NodeView(key, _sqlitegraph=self.sqlitegraph)

    def __iter__(self):
        query = self.sqlitegraph.conn.execute("SELECT _n FROM nodes")
        return (row["_n"] for row in query)

    def __len__(self):
        query = self.sqlitegraph.conn.execute("SELECT count(*) count FROM nodes")
        return query.fetchone()["count"]


class Nodes(MutableMapping):
    """A mapping from node IDs to nodes. Used by NetworkX classes to iterate over and
    insert nodes.

    :param _sqlitegraph: The SQLite-backed graph class.
    :type _sqlitegraph: entwiner.SqliteGraph

    """

    def __init__(self, _sqlitegraph=None, *args, **kwargs):
        self.sqlitegraph = _sqlitegraph

    def __getitem__(self, key):
        return Node(key, _sqlitegraph=self.sqlitegraph)

    def __iter__(self):
        query = self.sqlitegraph.conn.execute("SELECT _n FROM nodes")
        return (row["_n"] for row in query)

    def __len__(self):
        query = self.sqlitegraph.conn.execute("SELECT count(*) count FROM nodes")
        return query.fetchone()["count"]

    def __setitem__(self, key, ddict):
        if key in self:
            self.sqlitegraph.update_node(key, ddict)
        else:
            self.sqlitegraph.add_node(key, ddict)

    def __delitem__(self, key):
        if key in self:
            self.sqlitegraph.delete_node(key)
        else:
            raise KeyError(key)


class NodeView(Mapping):
    """Retrieves node attributes from table, but does not allow assignment.

    :param _sqlitegraph: The SQLite-backed graph class.
    :type _sqlitegraph: entwiner.SqliteGraph

    """

    def __init__(self, _n=None, _sqlitegraph=None, *args, **kwargs):
        self.n = _n
        self.sqlitegraph = _sqlitegraph

        if _n is not None and not self.sqlitegraph.get_node(_n):
            raise KeyError(f"Node {_n} not found")

    # TODO: consider that .items() requires two round trips - may want to override
    def __getitem__(self, key):
        return self.sqlitegraph.get_node(self.n)[key]

    def __iter__(self):
        return iter(self.sqlitegraph.get_node(self.n).keys())

    def __len__(self):
        return len(self.sqlitegraph.get_node(self.n))


# TODO: use Mapping (mutable?) abstract base class for dict-like magic
class Node(MutableMapping):
    """Retrieves mutable node attributes from table, but does not allow assignment.

    :param n: Node ID.
    :type n: str
    :param _sqlitegraph: The SQLite-backed graph class.
    :type _sqlitegraph: entwiner.SqliteGraph

    """

    def __init__(self, _n=None, _sqlitegraph=None, *args, **kwargs):
        self.n = _n
        self.sqlitegraph = _sqlitegraph

        if _n is not None:
            try:
                self.sqlitegraph.get_node(_n)
            except NodeNotFound:
                raise KeyError(f"Node {_n} not found")

    def __getitem__(self, key):
        return self.sqlitegraph.get_node(self.n)[key]

    def __iter__(self):
        return iter(self.sqlitegraph.get_node(self.n).keys())

    def __len__(self):
        return len(self.sqlitegraph.get_node(self.n))

    def __setitem__(self, key, value):
        self.sqlitegraph.set_node_attr(self.n, key, value)

    def __delitem__(self, key):
        if key in self:
            self.sqlitegraph.set_node_attr(self.n, key, None)
        else:
            raise KeyError(key)
