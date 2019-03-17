"""Wraps various readers/writers for different geospatial formats with a focus on
low-memory reading."""
import os

import fiona

from .exceptions import UnknownGeometry


def edge_generator(path, precision, rev=False, changes_sign=None):
    layer = os.path.splitext(os.path.basename(path))[0]
    if changes_sign is None:
        changes_sign = []
    with fiona.open(path) as handle:
        for f in handle:
            props = {k: v for k, v in f["properties"].items() if v is not None}
            props["_geometry"] = to_wkt(f["geometry"])
            props["_layer"] = layer
            props = {k: v for k, v in props.items() if v is not None}

            u = ", ".join(
                [str(round(c, precision)) for c in f["geometry"]["coordinates"][0]]
            )
            v = ", ".join(
                [str(round(c, precision)) for c in f["geometry"]["coordinates"][-1]]
            )
            yield u, v, props
            if rev:
                props = {**props}
                props["_geometry"] = to_wkt_rev(f["geometry"])
                for change_sign in changes_sign:
                    if change_sign in props:
                        props[change_sign] = -1 * props[change_sign]
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
