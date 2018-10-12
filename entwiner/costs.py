"""Cost function methods: adding weights to the edge table or dynamic costing"""

def shortest_path(u, v, d):
    if 'length' in d:
        return d['length']
    else:
        return 0
