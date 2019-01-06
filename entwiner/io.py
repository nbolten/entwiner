"""Wraps various readers/writers for different geospatial formats with a focus on
low-memory reading."""
import copy

import fiona


class UnknownGeometryError(ValueError):
    pass


def edge_generator(path, precision, rev=False):
    with fiona.open(path) as handle:
        for f in handle:
            props = dict(f["properties"])
            props["_geometry"] = to_wkt(f["geometry"])

            u = ", ".join(
                [str(round(c, precision)) for c in f["geometry"]["coordinates"][0]]
            )
            v = ", ".join(
                [str(round(c, precision)) for c in f["geometry"]["coordinates"][-1]]
            )
            yield u, v, props
            if rev:
                props = copy.deepcopy(props)
                props["_geometry"] = to_wkt_rev(f["geometry"])
                yield v, u, props


def to_wkt(geom):
    type_map = {"LineString": "LINESTRING"}
    geom_type = type_map.get(geom["type"], None)
    if geom_type is None:
        raise UnknownGeometryError()

    coords = ", ".join(
        [" ".join([str(p) for p in coord]) for coord in geom["coordinates"]]
    )
    return "{}({})".format(geom_type, coords)


def to_wkt_rev(geom):
    type_map = {"LineString": "LINESTRING"}
    geom_type = type_map.get(geom["type"], None)
    if geom_type is None:
        raise UnknownGeometryError()

    coords = ", ".join(
        [" ".join([str(p) for p in coord]) for coord in geom["coordinates"][::-1]]
    )
    return "{}({})".format(geom_type, coords)
