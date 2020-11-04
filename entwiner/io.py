"""Wraps various readers/writers for different geospatial formats with a focus
on low-memory reading."""
import os

import fiona

from .exceptions import UnrecognizedFileFormat


def edge_generator(path, precision, changes_sign=None, add_reverse=False):
    layer = os.path.splitext(os.path.basename(path))[0]
    if changes_sign is None:
        changes_sign = []

    def edge_from_feature(feature):
        props = {k: v for k, v in f["properties"].items() if v is not None}
        props["geom"] = f["geometry"]
        props["_layer"] = layer
        props = {k: v for k, v in props.items() if v is not None}

        u = ", ".join(
            [str(round(c, precision)) for c in f["geometry"]["coordinates"][0]]
        )
        v = ", ".join(
            [
                str(round(c, precision))
                for c in f["geometry"]["coordinates"][-1]
            ]
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
                if add_reverse:
                    props = {**props}
                    props["geom"] = reverse_linestring(
                        f["geometry"]["coordinates"]
                    )
                    for change_sign in changes_sign:
                        if change_sign in props:
                            props[change_sign] = -1 * props[change_sign]
                    yield v, u, props
    except fiona.errors.DriverError:
        raise UnrecognizedFileFormat(
            "{} has an unrecognized format.".format(path)
        )


def reverse_linestring(coords):
    return {"type": "LineString", "coordinates": list(reversed(coords))}
