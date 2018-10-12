import time

from entwiner import database

db = database.EdgeDB('sup.db')

print(db.columns())

t0 = time.time()
path = db.shortest_path([1000])
# print(path)
print(time.time() - t0)
print(len(path))

print(db.edges_by_nodes(7884, 666))
