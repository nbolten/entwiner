"""Outer adjacency lists. Must be compatible with holding either predecessors or
successors."""
from collections.abc import Mapping, MutableMapping

from .inner_adjlists import InnerPredecessorsView, InnerSuccessorsView
from .inner_adjlists import InnerPredecessors, InnerSuccessors


#
# Read-only outer adjacency mappings: views.
#


class OuterAdjlistView(Mapping):
    inner_adjlist_factory = InnerSuccessorsView
    iterator_str = "iter_successor_ids"
    size_str = "predecessors_len"

    def __init__(self, _sqlitegraph):
        self.sqlitegraph = _sqlitegraph

        self.inner_adjlist_factor = self.inner_adjlist_factory
        self.iterator = getattr(self.sqlitegraph, self.iterator_str)
        self.size = getattr(self.sqlitegraph, self.size_str)

    def __getitem__(self, key):
        return self.inner_adjlist_factory(self.sqlitegraph, key)

    def __iter__(self):
        # This method is overridden to avoid two round trips to the database.
        return self.iterator()

    def __len__(self):
        return self.size()

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return (
            (n, self.inner_adjlist_factory(_sqlitegraph=self.sqlitegraph, _n=n))
            for n in self.iterator()
        )

    def __contains__(self, key):
        # This method is overridden because __getitem__ doesn't  initially check for
        # a key's presence.
        # FIXME: should __getitem__ initially check for a key's presence?
        return self.sqlitegraph.has_node(key)


class OuterSuccessorsView(OuterAdjlistView):
    pass


class OuterPredecessorsView(OuterAdjlistView):
    inner_adjlist_factory = InnerPredecessorsView
    iterator_str = "iter_predecessor_ids"
    size_str = "successors_len"


#
# Writeable outer adjacency mappings.
#


class OuterSuccessors(OuterSuccessorsView, MutableMapping):
    inner_adjlist_factory = InnerSuccessors

    def __setitem__(self, key, ddict):
        self.sqlitegraph.replace_successors(key, ((k, v) for k, v in ddict.items()))

    def __delitem__(self, key):
        self.sqlitegraph.delete_successors(key)


class OuterPredecessors(OuterPredecessorsView, MutableMapping):
    inner_adjlist_factory = InnerPredecessors

    def __setitem__(self, key, ddict):
        self.sqlitegraph.replace_predecessors(key, ((k, v) for k, v in ddict.items()))

    def __delitem__(self, key):
        self.sqlitegraph.delete_predecessors(key)
