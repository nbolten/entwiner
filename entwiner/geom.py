"""Geometry helper methods."""


def wkt_linestring(coords):
    coords_str = ", ".join((" ".join((str(p) for p in coord)) for coord in coords))
    return f"LINESTRING({coords_str})"


def wkt_point(lon, lat):
    return f"POINT({lon} {lat})"
