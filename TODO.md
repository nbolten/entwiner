# Project Scoping

Entwiner is a network library for transportation data that needs to live on disk. It
can consume various forms of geospatial data - GeoJSON, OpenStreetMap, GTFS, etc. and
consolidate them in an SQLite database that can be shared with others and used for
graph theoretic analysis, like finding shortest paths.

Entwiner works with modular routing engines that can use its SQLite data format, and
ships with a Python-based one for use in research.

# Bugs / Design

## Separate workflow for database creation
Right now, specifying a database path that doesn't yet exist results in the creation of
an SQLite table there. We should have separate workflows for accessing an existing
'graph' database vs. creating a new one so that people don't accidentally create new
databases and so that we can catch bad paths when attempting to connect to an existing
one. Example API: digraphdb.create() and digraphdb.connect().

## Faster adding of edges
Adding edges is needlessly slow. It should only take a second or two to do all of
Seattle's AccessMap data, but it takes 10-30.

# Ideas

## Backing for NetworkX graphs

The SQLite databases produced by entwiner should be able to serve as a persistent data
store for NetworkX graphs in general. Roll into its own package? Should also offer a
three-column serialized keys-values option as well. i.e. a schema strategy: this
implies the need for a metadata table or two, with encoding schemes and column
definitions. Could borrow from GeoPackage (an actual table in the database) or
Datasette (a metadata json).

## Pluggable non-Python routing engines

Python is great for lots of things, but is not particularly fast for implementing
things like Dijkstra's algorithm. It would be great to have more implementations -
they'd be faster and would also help enforce standard formatting. e.g. Go or Rust
implementations of Dijkstra that work on in-memory or on-disk versions of the graph.

## Interface with other SQLite + Python projects

- Datasette will create a read-only SQL query + JSON response API + nifty web
  interface out of an SQL database. Users should be encouraged to use datasette or
  we could even add an 'easy mode' command that uses it (optionally).

- GeoPackages are an OGC data standard for self-describing geospatial datasets that is
  really just an SQLite database. The databases produced by entwiner should borrow
  some of this approach, or could even just be a geopackage with an extra edge table.
