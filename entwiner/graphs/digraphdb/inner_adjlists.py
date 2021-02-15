"""Inner adjacency lists."""
from collections.abc import Mapping, MutableMapping
from functools import partial

from .edges import Edge, EdgeView


#
# Read-only inner adjacency mappings: views.
#


class InnerAdjlistView(Mapping):
    edge_factory = EdgeView
    id_iterator_str = "successor_nodes"
    iterator_str = "successors"
    size_str = "unique_successors"

    def __init__(self, _network, _n):
        self.network = _network
        self.n = _n

        self.edge_factory = partial(self.edge_factory, _network=_network)
        self.id_iterator = getattr(self.network.edges, self.id_iterator_str)
        self.iterator = getattr(self.network.edges, self.iterator_str)
        self.size = getattr(self.network.edges, self.size_str)

    def __getitem__(self, key):
        return self.edge_factory(_u=self.n, _v=key)

    def __iter__(self):
        return iter(self.id_iterator(self.n))

    def __len__(self):
        return self.size(self.n)

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return (
            (v, self.edge_factory(**row)) for v, row in self.iterator(self.n)
        )


class InnerSuccessorsView(InnerAdjlistView):
    pass


class InnerPredecessorsView(InnerAdjlistView):
    id_iterator_str = "predecessor_nodes"
    iterator_str = "predecessors"
    size_str = "unique_predecessors"


#
# Writeable outer adjacency mappings.
#
class InnerSuccessors(InnerSuccessorsView, MutableMapping):
    edge_factory = Edge

    def __init__(self, _network, _n):
        super().__init__(_network=_network, _n=_n)
        self.edge_factory = partial(Edge, _network=_network)

    def __setitem__(self, key, ddict):
        self.network.insert_or_replace_edge(self.n, key, ddict, commit=True)

    def __delitem__(self, key):
        self.network.delete_edges((self.n, key))

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return (
            (v, self.edge_factory(_network=self.network, _u=self.n, _v=v))
            for v, row in self.iterator(self.n)
        )


class InnerPredecessors(InnerPredecessorsView, MutableMapping):
    edge_factory = Edge

    def __setitem__(self, key, ddict):
        self.network.insert_or_replace_edge(key, self.n, ddict, commit=True)

    def __delitem__(self, key):
        self.network.delete_edges((key, self.n))

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return (
            (u, self.edge_factory(_network=self.network, _u=u, _v=self.n))
            for u, row in self.iterator(self.n)
        )
