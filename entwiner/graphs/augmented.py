import itertools

import networkx as nx


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
