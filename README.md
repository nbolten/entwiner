# Note: this is under very active and early development. Don't use for production
# applications yet!

# Entwiner

Build, use, and share routable transportation graphs using common geospatial data.

## Installation and Usage

### Installation

If you use `pip` version 19 or greater, `entwiner` can be installed directly:

`pip install git+https://github.com/nbolten/entwiner`

If you have a version of `pip` lower than 19, you can install using `poetry` or render
a pip-installable sdist using `poetry build`, which creates a pip-installable
`dist/entwiner-*.tar.gz`.

### Initial usage

Create a database from GeoJSON data using this pattern:

`entwiner [INFILES]... OUTFILE.db`

where `INFILES` is any number of linear GeoJSON featurecollections.

You now have an SQLite (technically, Spatialite) database (`OUTFILE.db`) with two
tables that describe a directed graph: `nodes` and `edges`.

### The transportation network database graph

The `nodes` table is the list of graph nodes
representing the points where your input lines meet one another, and contains only
these columns:

- `rowid` (a unique integer, default in SQLite tables)

- `lon` (longitude)

- `lat` (latitude).

The `edges` table contains these columns:

- `rowid` (a unique integer, default in SQLite tables)

- `u`: the "start node" of this edge.

- `v`: the "end node" of this edge.

- `*`: all of the "flat" data from the GeoJSON features, encoded as columnar data. The
types will be automatically converted to SQLite types, and have `null` where missing.

### Using the database

The database can be used by any applications that consume node/edge information, but
`entwiner` comes with some convenient Python interfaces for doing network analysis and
routing.

`entwiner.database.DiGraphDB(path)`: if the db already exists, this creates a (somewhat)
networkx-compatible interface to a DiGraph. As of writing, it is compatible with
shortest-path algorithms and centrality metrics. Example:

    import entwiner as ent
    import networkx as nx

    G = ent.graphs.digraphdb.DiGraphDB(path='test.db')
    path = nx.algorithms.shortest_paths.dijkstra_path(G, '-122.5049849, 48.7798528', '-122.5074134, 48.7798173', 'length')
    print(path)

## Why SQLite?

- It's everywhere.

- It's just one file!

- It handles locking for you (works great with web apps)

- Flexible persistence: it can live on disk (default), you can set the cache size high
  so that repeated queries happen in-memory, and you can set the whole thing to live
  in-memory (fastest)
