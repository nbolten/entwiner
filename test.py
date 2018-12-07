import time

import networkx as nx

from entwiner import database, graphs

START = '-122.5049849, 48.7798528'
END = '-122.5091418, 48.7789324'

G = graphs.digraphdb.DiGraphDB(database='sup.db')


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
