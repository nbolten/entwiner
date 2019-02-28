"""Reusable sqlite-backed Node container(s)."""
from .exceptions import NodeNotFound


class ReadOnlyNode:
    def __init__(self, _graphdb=None, *args, **kwargs):
        self.graphdb = _graphdb

    def __getitem__(self, key):
        return self.graphdb.get_node(key)

    def __contains__(self, key):
        try:
            self[key]
        except NodeNotFound:
            return False
        return True

    def __iter__(self):
        query = self.graphdb.conn.execute("SELECT _key FROM nodes")
        return (row[0] for row in query)

    def __len__(self):
        query = self.graphdb.conn.execute("SELECT count(*) FROM nodes")
        return query.fetchone()[0]


# TODO: use Mapping (mutable?) abstract base class for dict-like magic
class Node(ReadOnlyNode):
    def clear(self):
        # FIXME: make this do something
        pass

    def __setitem__(self, key, ddict):
        if key in self:
            self.graphdb.update_node(key, ddict)
        else:
            self.graphdb.add_node(key, ddict)


def node_factory_factory(graphdb, readonly=False):
    """Creates factories of DB-based Nodes.

    :param graphdb: Graph database object.
    :type graphdb: entwiner.GraphDB

    """

    def node_factory():
        return Node(_graphdb=graphdb)

    def readonly_node_factory():
        return ReadOnlyNode(_graphdb=graphdb)

    if readonly:
        return readonly_node_factory
    else:
        return node_factory
