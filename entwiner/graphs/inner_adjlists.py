"""Inner adjacency lists."""
from collections.abc import Mapping, MutableMapping
from functools import partial

from .edges import Edge, EdgeView


#
# Read-only inner adjacency mappings: views.
#


class InnerAdjlistView(Mapping):
    edge_factory = EdgeView
    id_iterator_str = "iter_successor_ids"
    iterator_str = "iter_successors"
    size_str = "len_successors_of"

    def __init__(self, _sqlitegraph, _n):
        self.sqlitegraph = _sqlitegraph
        self.n = _n

        self.edge_factory = partial(self.edge_factory, _sqlitegraph=_sqlitegraph)
        self.id_iterator = getattr(self.sqlitegraph, self.id_iterator_str)
        self.iterator = getattr(self.sqlitegraph, self.iterator_str)
        self.size = getattr(self.sqlitegraph, self.size_str)

    def __getitem__(self, key):
        return self.edge_factory(_u=self.n, _v=key)

    def __iter__(self):
        return self.id_iterator(self.n)

    def __len__(self):
        return self.size(self.n)

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return ((v, self.edge_factory(**row)) for v, row in self.iterator(self.n))


class InnerSuccessorsView(InnerAdjlistView):
    pass


class InnerPredecessorsView(InnerAdjlistView):
    id_iterator_str = "iter_predecessor_ids"
    iterator_str = "iter_predecessors"
    size_str = "len_predecessors_of"


#
# Writeable outer adjacency mappings.
#
class InnerSuccessors(InnerSuccessorsView, MutableMapping):
    edge_factory = Edge

    def __init__(self, _sqlitegraph, _n):
        super().__init__(_sqlitegraph=_sqlitegraph, _n=_n)
        self.edge_factory = partial(Edge, _sqlitegraph=_sqlitegraph)

    def __setitem__(self, key, ddict):
        self.sqlitegraph.insert_or_replace_edge(self.n, key, ddict, commit=True)

    def __delitem__(self, key):
        self.sqlitegraph.delete_edges((self.n, key))

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return (
            (v, self.edge_factory(_sqlitegraph=self.sqlitegraph, _u=self.n, _v=v))
            for v, row in self.iterator(self.n)
        )


class InnerPredecessors(InnerPredecessorsView, MutableMapping):
    edge_factory = Edge

    def __setitem__(self, key, ddict):
        self.sqlitegraph.insert_or_replace_edge(key, self.n, ddict, commit=True)

    def __delitem__(self, key):
        self.sqlitegraph.delete_edges((key, self.n))

    def items(self):
        # This method is overridden to avoid two round trips to the database.
        return (
            (u, self.edge_factory(_sqlitegraph=self.sqlitegraph, _u=u, _v=self.n))
            for u, row in self.iterator(self.n)
        )
