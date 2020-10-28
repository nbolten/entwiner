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
    iterator_str = "predecessor_nodes"
    size_str = "unique_predecessors"

    def __init__(self, _network):
        self.network = _network

        self.inner_adjlist_factor = self.inner_adjlist_factory
        self.iterator = getattr(self.network.edges, self.iterator_str)
        self.size = getattr(self.network.edges, self.size_str)

    def __getitem__(self, key):
        return self.inner_adjlist_factory(self.network, key)

    def __iter__(self):
        # This method is overridden to avoid two round trips to the database.
        return self.iterator()

    def __len__(self):
        return self.size()

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return (
            (n, self.inner_adjlist_factory(_network=self.network, _n=n))
            for n in self.iterator()
        )

    def __contains__(self, key):
        # This method is overridden because __getitem__ doesn't  initially check for
        # a key's presence.
        # FIXME: should __getitem__ initially check for a key's presence?
        return self.network.has_node(key)


class OuterSuccessorsView(OuterAdjlistView):
    pass


class OuterPredecessorsView(OuterAdjlistView):
    inner_adjlist_factory = InnerPredecessorsView
    iterator_str = "successor_nodes"
    size_str = "unique_successors"


#
# Writeable outer adjacency mappings.
#


class OuterSuccessors(OuterSuccessorsView, MutableMapping):
    inner_adjlist_factory = InnerSuccessors

    def __setitem__(self, key, ddict):
        self.network.replace_successors(key, ((k, v) for k, v in ddict.items()))

    def __delitem__(self, key):
        self.network.delete_successors(key)


class OuterPredecessors(OuterPredecessorsView, MutableMapping):
    inner_adjlist_factory = InnerPredecessors

    def __setitem__(self, key, ddict):
        self.network.replace_predecessors(key, ((k, v) for k, v in ddict.items()))

    def __delitem__(self, key):
        self.network.delete_predecessors(key)
