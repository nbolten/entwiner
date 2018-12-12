import time

import networkx as nx

from entwiner import graphs

START = '-122.2370805, 47.5096408'
END = '-122.2410717, 47.5110904'

G = graphs.digraphdb.DiGraphDB(database='test.db')


t0 = time.time()

# cost_fun = lambda u, v, d: d.get('length', 0)
x = nx.algorithms.shortest_paths.dijkstra_path(G, START, END, 'length')
print(x)
print(list(G[END].items()))
# print(time.time() - t0)
# print([(k, d) for k, d in G._succ[START].items()])

# for k, v in G[START].items():
#     print(k)
#     print(v)
#     print({k1: v1 for k1, v1 in v.items()})
#     print('x')

# print(nx.algorithms.centrality.degree_centrality(db))
# deg = G.degree()
# nx.algorithms.all_pairs_node_connectivity(G)

# print('Importing graph')
print(len(G))

# @profile
# def to_nx(G):
#     return nx.DiGraph(G)

# G2 = to_nx(G)
