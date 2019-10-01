"""Wraps various readers/writers for different geospatial formats with a focus on
low-memory reading."""
import os

import fiona

from .exceptions import UnknownGeometry, UnrecognizedFileFormat


def edge_generator(path, precision, rev=False, changes_sign=None):
    layer = os.path.splitext(os.path.basename(path))[0]
    if changes_sign is None:
        changes_sign = []

    def edge_from_feature(feature):
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

        return u, v, props

    try:
        with fiona.open(path) as handle:
            for f in handle:
                # TODO: log total number of edges skipped and inform user.
                # TODO: split MultiLineStrings into multiple LineStrings?
                if f["geometry"]["type"] != "LineString":
                    continue
                u, v, props = edge_from_feature(f)
                yield u, v, props
                if rev:
                    props = {**props}
                    props["_geometry"] = to_wkt_rev(f["geometry"])
                    for change_sign in changes_sign:
                        if change_sign in props:
                            props[change_sign] = -1 * props[change_sign]
                    yield v, u, props
    except fiona.errors.DriverError:
        raise UnrecognizedFileFormat("{} has an unrecognized format.".format(path))


def to_wkt(geom):
    coords = ", ".join(
        [" ".join([str(p) for p in coord]) for coord in geom["coordinates"]]
    )
    return "LINESTRING({})".format(coords)


def to_wkt_rev(geom):
    coords = ", ".join(
        [" ".join([str(p) for p in coord]) for coord in geom["coordinates"][::-1]]
    )
    return "LINESTRING({})".format(coords)
