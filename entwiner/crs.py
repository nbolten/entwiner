"""Handle coordinate reference systems and transformations"""
import pyproj


def project(coordinates, from_crs=None, to_crs=None):
    if to_crs is None:
        if from_crs is None:
            raise ValueError("from_crs or to_crs must be set")
        to_crs = 'epsg:4326'

    from_proj = pyproj.Proj(init=from_crs)
    to_proj = pyproj.Proj(init=to_crs)

    new_coords = []
    for lon, lat in coordinates:
        new_coords.append(pyproj.transform(from_proj, to_proj, lon, lat))

    return new_coords
