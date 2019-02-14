# Bugs

## Extension issue

sqlite3.Connection.load_extension doesn't exist for some users. Figure out why and/or
just use .execute("SELECT load_extension('whatever.so')")

## Node attributes

Nodes currently lack most attributes, but it's useful to have them for queries and
filtering (e.g. having node types, compatibility with OSM data, etc.). Add support for
node attributes.

## Defining node and edge keys

The current strategy uses lon-lat coordinates to uniquely define nodes and edges. This
seems like a pretty decent way of handling identity, but I can still see it being
potentially useful to have the option to relabel into integers or associate an `_id`
column of integers.

# Project Scoping

## Basic

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

## Graph manipulation / joins

### Spatial joins

It is not uncommon to want to join other datasets to a transportation network.
Examples:

- GTFS and/or other transportation feeds (bus stops, etc.)
- POIs in general: trees, mailboxes, businesses, etc.
- GPS data (use something like OpenLR?)

`entwiner` can leverage spatial indices in the database to do fast lookups and
associations, as well as SQL queries to filter joins. We should add some simple join
functionality.

### Whole-graph manipulations

There are some pretty common graph manipulations regarding transportation networks:

- Deriving a minor (edges --> nodes)
    - This includes transforming intersection-to-intersection graphs into a graph of
    maneuvers (turn right, turn left, etc.)

- Contraction hierarchies

- Other things

These seem like useful manipulations to add to `entwiner` for use by other packages,
including the AccessMap APIs.

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
