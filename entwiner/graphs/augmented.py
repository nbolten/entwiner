import itertools

import networkx as nx


"""
The purpose of these wrappers / masks is to act exactly like their masked graph
object, just with 'temporary' edges stored in another place. This may be very
difficult, so this docstring will contain ideas for how to implement such a mask.

Requirements:

1) The 'mask' data must be stored separately and presumably in-memory.

2) The object must act exactly like the graph it's masking. What this means:
    A) Edges and nodes can be added using the same interfaces: .add_edges, .add_nodes,
    .add_edges_from.
        i) Adding an edge implies more relationships than might be expected:
            a) The edge from u1 to v1 and any associated data such that G[u1] shows
            v1 as well as whatever other edges are adjacent in the 'real' graph.
            b) u1 must be accessible as a predecessor of v1.
            c) If the Graph is undirected, v1 -> u1 also needs to be added.
        ii) There are low-level interfaces for networkx graphs, like the generators
        for nodes, edges, and adjacencies, that are used repeatedly when adding or
        accessing edges. It would be best to interface at this level for
        generalizability and potentially a smaller (and easier to debug) interface.
    B) Edges can be accessed via the iterable .edges() and via __getitem__ on the
    graph object.

"""


class MaskedMapping:
    def __init__(self, mapping, mask_factory=dict, mask_defaults=None):
        self.mapping = mapping
        self.mask_factory = mask_factory
        self.mask = self.mask_factory()
        if mask_defaults is not None:
            for k, v in mask_defaults.items():
                self.mask[k] = v

    def __getitem__(self, key):
        try:
            return self.mask[key]
        except KeyError:
            return self.mapping[key]

    def __len__(self):
        return len(self.mask) + len(self.mapping)

    def __iter__(self):
        return itertools.chain(iter(self.mask), iter(self.maping))

    def copy(self):
        return MaskedMapping(self.mapping, mask_factory=self.mask_factory, mask_defaults=self.mask)


class AugmentedDiGraphView:
    def __init__(self, G, edges):
        self.G = G
        self.edges = edges
        self.adj = MaskedMapping(self.G.adj)

        if self.is_directed():
            if self.is_multigraph():
                self.G_aug = nx.MultiDiGraph()
            else:
                self.G_aug = nx.DiGraph()
            self.succ = MaskedMapping(self.G.succ)
        else:
            if self.is_multigraph():
                self.G_aug = nx.MultiGraph()
            else:
                self.G_aug = nx.Graph()

        self.G_aug.add_edges_from(edges)

    def is_directed(self):
        return self.G.is_directed()

    def is_multigraph(self):
        return self.G.is_multigraph()

    @property
    def adj(self):
        try:
            return self.G_aug.adj
        except:
            return self.G.adj

    @property
    def succ(self):
        try:
            return self.G_aug.succ
        except:
            return self.G.succ

    def __getitem__(self, key):
        try:
            return self.G_aug[key]
        except KeyError:
            return self.G[key]
