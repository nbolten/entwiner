# Project Scoping

Entwiner is a network library for transportation data that needs to live on disk. It
can consume various forms of geospatial data - GeoJSON, OpenStreetMap, GTFS, etc. and
consolidate them in an SQLite database that can be shared with others and used for
graph theoretic analysis, like finding shortest paths.

The database can be used in several ways:
    - As an SQL database (of course).
    - As an exchange format for sending/receiving transportation networks.
    - In combination with datasette to share and query transportation networks.
    - Investigated and upated via a networkx-compatible Python interface (currently
      DiGraphDB, which is equivalent to networkx.DiGraph). Can be transferred to
      memory through the standard networkx method for copying: nx.DiGraph(G).
    - In combination with the built-in routing engine to do shortest-path analytics and
      web services.
    - Copied to memory and fed into any other graph framework.

# Bugs

## Empty column values should not be None
There's an awkward interface where sqlite3.Row returns None when coerced to a
dict-like. These should be dropped, at least optionally.

# Ideas

## Spin off SQLite-backed networkx graphs

The graphs module could be its own thing - anyone who has a large-ish graph might want
to have it live on-disk. Super-fast cloud SSDs might make this production-ready in
certain situations.

## Alternative schemas

The current strategy is 'expanded': a new column is created for every property in the
source data. This is great for human readability and SQL queries, but users may want
more flexibility - like storing data that doesn't fit neatly into an SQLite column
type. One option is to instead use a TEXT column and (de)serialize all the properties
to it, with multiple encoding options. e.g. json or pickle etc.

## Data exchange format

With maybe some more metadata, the database could be its own exchange format / have a
different extension to ease sharing. Would ideally have a value-add, like a metadata
table that assists with graphy things. Look into geopackage - is it actually an SQLite
db, or is it just derived from the spec?

## Datasette integration

Anything we need to do for this? Seems like it should be automatic - just add to docs.

## Pluggable non-Python routing engines

Python is great for lots of things, but is not particularly fast for implementing
things like Dijkstra's algorithm. It would be great to have more implementations -
they'd be faster and would also help enforce standard formatting. e.g. Go or Rust
implementations of Dijkstra that work on in-memory or on-disk versions of the graph.
