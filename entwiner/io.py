"""Wraps various readers/writers for different geospatial formats with a focus on
low-memory reading."""

import json
from . import crs


class InvalidFormatError(ValueError):
    """Entwine was not able to read this format."""


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
    # FIXME: re-enable once CRS can be extracted from GeoJSON
    # for feature in geojson['features']:
    #     feature['geometry']['coordinates'] = crs.project(feature['geometry']['coordinates'])
    return iter(geojson['features'])
