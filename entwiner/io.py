"""Wraps various readers/writers for different geospatial formats with a focus on
low-memory reading."""
import json

from shapely import geometry

from . import crs


PRECISION = 7


class InvalidFormatError(ValueError):
    """Entwiner was not able to read this format."""


def edge_generator(feature_gen):
    def get_node(feature, index):
        coords = feature["geometry"]["coordinates"][index]
        point = [str(round(c, PRECISION)) for c in coords]
        return ", ".join(point)

    def generate_attribs(feature):
        attrib = feature["properties"]
        attrib["_geometry"] = geometry.shape(feature["geometry"]).wkt
        return attrib

    return ((get_node(f, 0), get_node(f, -1), generate_attribs(f)) for f in feature_gen)


def feature_generator(path):
    if path.endswith("geojson"):
        # Use GeoJSON reader
        return read_geojson(path)
    else:
        raise InvalidFormatError("{} not recognized as valid input format".format(path))


def read_geojson(path):
    # FIXME: this strategy is not safe for low-memory situations. Are there any
    # strategies for incrementally loading GeoJSON?
    with open(path) as f:
        geojson = json.load(f)

    # Convert to lon-lat
    # TODO: extract CRS from GeoJSON or assume it's already lon-lat
    return iter(geojson["features"])
