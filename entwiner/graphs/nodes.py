"""Reusable sqlite-backed Node container(s)."""
from ..exceptions import NodeNotFound


class ImmutableNode:
    def __init__(self, _sqlitegraph=None, *args, **kwargs):
        self.sqlitegraph = _sqlitegraph

    def __getitem__(self, key):
        return self.sqlitegraph.get_node(key)

    def __contains__(self, key):
        try:
            self[key]
        except NodeNotFound:
            return False
        return True

    def __iter__(self):
        query = self.sqlitegraph.conn.execute("SELECT _key FROM nodes")
        return (row["_key"] for row in query)

    def __len__(self):
        query = self.sqlitegraph.conn.execute("SELECT count(*) count FROM nodes")
        return query.fetchone()["count"]


# TODO: use Mapping (mutable?) abstract base class for dict-like magic
class Node(ImmutableNode):
    def clear(self):
        # FIXME: make this do something
        pass

    def __setitem__(self, key, ddict):
        if key in self:
            self.sqlitegraph.update_node(key, ddict)
        else:
            self.sqlitegraph.add_node(key, ddict)


def node_factory_factory(sqlitegraph):
    """Creates factories of DB-based Nodes.

    :param sqlitegraph: Graph database object.
    :type sqlitegraph: entwiner.GraphDB

    """

    def node_factory():
        return Node(_sqlitegraph=sqlitegraph)

    return node_factory


def immutable_node_factory_factory(sqlitegraph):
    """Creates factories of immutable DB-based Nodes.

    :param sqlitegraph: Graph database object.
    :type sqlitegraph: entwiner.GraphDB

    """

    def node_factory():
        return ImmutableNode(_sqlitegraph=sqlitegraph)

    return node_factory
