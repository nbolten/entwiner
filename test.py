import time

import networkx as nx

from entwiner import database

db = database.DiGraphDB('sup.db')

# print(db.columns())
#
t0 = time.time()
# path = db.shortest_path([1000])
# # print(path)
# print(len(path))

# print(db.edges_by_nodes(7884, 666))

# print(db[7884])

print(7884 in db)

cost_fun = lambda u, v, d: d.get('length', 0)
# nx.algorithms.shortest_paths.weighted._dijkstra_multisource(db, [7884], cost_fun)
nx.algorithms.shortest_paths.dijkstra_path(db, 7884, 666, 'length')
print(time.time() - t0)

print(db[7884][666])

# print(nx.algorithms.centrality.degree_centrality(db))
# deg = db.degree()
# print(next(deg))
