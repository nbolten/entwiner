# Entwiner

Build, use, and share routable transportation graphs using common geospatial data.

## Why SQLite?

- It's everywhere.

- It's just one file!

- It handles locking for you (works great with web apps)

- Flexible persistence: it can live on disk (default), you can set the cache size high
  so that repeated queries happen in-memory, and you can set the whole thing to live
  in-memory (fastest)
