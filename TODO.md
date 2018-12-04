# Project Scoping

Entwiner is a network library for transportation data that needs to live on disk. It
can consume various forms of geospatial data - GeoJSON, OpenStreetMap, GTFS, etc. and
consolidate them in an SQLite database that can be shared with others and used for
graph theoretic analysis, like finding shortest paths.

Entwiner works with modular routing engines that can use its SQLite data format, and
ships with a Python-based one for use in research.


# Ideas

## Backing for NetworkX graphs

The SQLite databases produced by entwiner should be able to serve as a persistent data
store for NetworkX graphs, albeit with a few constraints on associated attributes and
the data types for node references. Aside from being cool (much larger graphs with
NetworkX), this would also make all of the algorithms implemented for NetworkX
available for entwiner

## Spatialite - optional

## Pluggable non-Python routing engines

Python is great for lots of things, but is not particularly fast for implementing
things like Dijkstra's algorithm. It would be great to have more implementations -
they'd be faster and would also help enforce standard formatting.

## Interface with other SQLite + Python projects

- Datasette will create a read-only SQL query + JSON response API + nifty web
  interface out of an SQL database. Users should be encouraged to use datasette or
  we could even add an 'easy mode' command that uses it (optionally).

- GeoPackages are an OGC data standard for self-describing geospatial datasets that is
  really just an SQLite database. The databases produced by entwiner should borrow
  some of this approach, or could even just be a geopackage with an extra edge table.
