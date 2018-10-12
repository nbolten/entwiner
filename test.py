import time

from entwiner import database

db = database.EdgeDB('sup.db')

t0 = time.time()
path = db.shortest_path([1000])
print(time.time() - t0)
print(len(path))
