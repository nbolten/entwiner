from collections import OrderedDict
import copy

import pandas as pd
import partridge as ptg
from shapely.geometry import mapping, shape, Point


# 10-meter snap distance for stops
SNAP_DISTANCE = 10


def add_gtfs(gpkgn, path, name, filter_func=None):
    feed = ptg.load_feed(path)

    # FIXME: many of these tables do not have a geospatial component and should
    #        not use a FeatureTable, but a base table that doesn't register
    #        with any core gpkg_* tables.

    # TODO: create an actual strategy for how to handle adding feeds and what
    #       to do on conflicts.
    drop = ("stops", "routes", "trips", "stop_times", "shapes")
    for d in drop:
        table_name = f"gpkg_gtfs_{d}"
        if table_name in gpkgn.gpkg.feature_tables:
            gpkgn.gpkg.drop_feature_table(table_name)

    #
    # Add stops
    #
    stops_table = gpkgn.gpkg.add_feature_table(
        "gpkg_gtfs_stops", "POINT", srid=4326
    )

    # Enforce that on name conflict, either raise exception or overwrite all
    # rows with same name
    def extract_stops(row):
        d = OrderedDict()
        d["geom"] = {
            "type": "Point",
            "coordinates": (row["stop_lon"], row["stop_lat"]),
        }
        for key in ("stop_id", "stop_name"):
            d[key] = row[key]
        d["name"] = name

        return d

    stop_features = list(feed.stops.apply(extract_stops, axis=1))
    stops_table.write_features(stop_features)

    #
    # Add routes
    #
    routes_table = gpkgn.gpkg.add_feature_table(
        "gpkg_gtfs_routes", "POINT", srid=4326
    )

    # Enforce that on name conflict, either raise exception or overwrite all
    # rows with same name
    def extract_routes(row):
        d = OrderedDict()
        for key in (
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
        ):
            d[key] = row[key]
        d["name"] = name

        return d

    route_features = feed.routes.apply(extract_routes, axis=1)
    routes_table.write_features(route_features)

    #
    # Add trips
    #
    trips_table = gpkgn.gpkg.add_feature_table(
        "gpkg_gtfs_trips", "POINT", srid=4326
    )

    # Enforce that on name conflict, either raise exception or overwrite all
    # rows with same name
    def extract_trips(row):
        d = OrderedDict()
        for key in (
            "route_id",
            "service_id",
            "trip_id",
            "shape_id",
        ):
            d[key] = row[key]
        d["name"] = name

        return d

    route_features = feed.trips.apply(extract_trips, axis=1)
    trips_table.write_features(route_features)

    #
    # Add stop_times
    #
    stop_times_table = gpkgn.gpkg.add_feature_table(
        "gpkg_gtfs_stop_times", "POINT", srid=4326
    )

    # Enforce that on name conflict, either raise exception or overwrite all
    # rows with same name
    def extract_stop_times(row):
        d = OrderedDict()
        for key in (
            "trip_id",
            "stop_id",
            "stop_sequence",
        ):

            d[key] = row[key]
            d["name"] = name

        return d

    route_features = feed.stop_times.apply(extract_stop_times, axis=1)
    stop_times_table.write_features(route_features)

    #
    # Add shapes
    #
    shapes_table = gpkgn.gpkg.add_feature_table(
        "gpkg_gtfs_shapes", "LINESTRING", srid=4326
    )

    def coord_pair_from_pts(row):
        return [row["shape_pt_lon"], row["shape_pt_lat"]]

    def process_shape(grp):
        ordered = grp.sort_values("shape_pt_sequence")
        coords = list(ordered.apply(coord_pair_from_pts, axis=1))
        return pd.DataFrame(
            [{"geom": {"type": "LineString", "coordinates": coords}}]
        )

    shape_groups = feed.shapes.groupby("shape_id")
    shapes = shape_groups.apply(process_shape).reset_index()

    # Enforce that on name conflict, either raise exception or overwrite all
    # rows with same name
    def extract_shapes(row):
        d = OrderedDict()
        for key in (
            "geom",
            "shape_id",
        ):
            d[key] = row[key]
        d["name"] = name

        return d

    shape_features = shapes.apply(extract_shapes, axis=1)
    shapes_table.write_features(shape_features)

    if filter_func is None:

        def filter_func(stop):
            return True

    # Snap stops to edges and split at those nodes.
    # FIXME: this functionality almost certainly requires a filtering step
    #        that should be user-controllable. Filters can get very complicated
    #        quickly. This argues for a different splitting of responsibilities
    #        between entwiner and unweaver.
    for stop in stop_features:
        # TODO: batch writes / drops and do all at once

        # FIXME: This should only happen when using a GeoPackageNetwork /
        #        network-enabled geopackage and possibly only when a flag is
        #        set.
        edge_table = gpkgn.edges
        lon, lat = stop["geom"]["coordinates"]
        nearest = edge_table.dwithin(lon, lat, SNAP_DISTANCE, sort=True)

        found = False
        for u, v, d in nearest:
            if filter_func(d):
                found = True
                break
        if not found:
            continue

        # FIXME: Handle case where nearest neighbor is a node.

        # u = nearest_edge.pop("_u")
        # v = nearest_edge.pop("_v")
        # d = nearest_edge

        stop_id = stop["stop_id"]
        point = Point(lon, lat)

        # Create split copy of nearest_edge. This needs to happen for both
        # directed edges.
        # TODO: Unweaver already has this functionality. Should merge packages
        #       together to remove redundancy.
        def split_edge(d, point):
            # FIXME: all of this should be done in a projection suitable for
            #        cartesian calculations
            edge_geom = shape(d["geom"])
            # FIXME: not in the right coordinate scheme for these units to be
            #        reasoned about
            projected_distance = edge_geom.project(point)
            if (projected_distance < 1e-6) or (
                projected_distance > (edge_geom.length - 1e-6)
            ):
                return None
            coords1, coords2 = cut(edge_geom, projected_distance)

            # Interpolate values that need to be interpolated (length?).
            # FIXME: Should be defined within data structure somewhere.
            d1 = copy.deepcopy(d)
            d2 = copy.deepcopy(d)
            d1["geom"] = {"type": "LineString", "coordinates": coords1}
            d2["geom"] = {"type": "LineString", "coordinates": coords2}

            return d1, d2

        split = split_edge(d, point)
        if split is None:
            continue

        d1, d2 = split
        _u, _v = v, u

        _d = edge_table.get_edge(v, u)
        _d1, _d2 = split_edge(_d, point)

        # TODO: dropping of edge should happen in same transaction as adding
        #       new edges to ensure db integrity

        edge_table.write_features(
            [
                {
                    **d1,
                    "_u": u,
                    "_v": stop_id,
                },
                {
                    **d2,
                    "_u": stop_id,
                    "_v": v,
                },
                {
                    **_d1,
                    "_u": _u,
                    "_v": stop_id,
                },
                {
                    **_d2,
                    "_u": stop_id,
                    "_v": _v,
                },
            ]
        )

        with gpkgn.gpkg.connect() as conn:
            # FIXME: there is no delete_edge or delete_feature function!?
            conn.execute("DELETE FROM edges WHERE _u = ? AND _v = ?", (u, v))
            conn.execute("DELETE FROM edges WHERE _u = ? AND _v = ?", (v, u))

    # return stops_table


def cut(line, distance):
    """Cuts a Shapely LineString at the stated distance. Returns a list of two
    new LineStrings for valid inputs. If the distance is 0, negative, or longer
    than the LineString, a list with the original LineString is produced.

    :param line: LineString to cut.
    :type line: shapely.geometry.LineString
    :param distance: Distance along the line where it will be cut.
    :type distance: float

    """
    if distance <= 0.0 or distance >= line.length:
        return list(line.coords)
    # coords = list(line.coords)
    coords = line.coords

    pd = 0
    last = coords[0]
    for i, p in enumerate(coords):
        if i == 0:
            continue
        pd += _point_distance(last, p)

        if pd == distance:
            return [coords[: i + 1], coords[i:]]
        if pd > distance:
            cp = line.interpolate(distance)
            return [coords[:i] + [(cp.x, cp.y)], [(cp.x, cp.y)] + coords[i:]]

        last = p
    # If the code reaches this point, we've hit a floating point error or
    # something, as the total estimated distance traveled is less than the
    # distance specified and the distance specified is less than the length of
    # the geometry, so there's some small gap. The approach floating around
    # online is to use linear projection to find the closest point to the given
    # distance, but this is not robust against complex, self-intersection
    # lines. So, instead: we just assume it's between the second to last and
    # last point.
    cp = line.interpolate(distance)
    return [coords[:i] + [(cp.x, cp.y)], [(cp.x, cp.y)] + coords[i:]]


def cut_off(line, distance):
    """Cuts a Shapely LineString at the stated distance. Returns a list of two
    new LineStrings for valid inputs. If the distance is 0, negative, or longer
    than the LineString, a list with the original LineString is produced.

    :param line: LineString to cut.
    :type line: shapely.geometry.LineString
    :param distance: Distance along the line where it will be cut.
    :type distance: float

    """
    if distance <= 0.0 or distance >= line.length:
        return list(line.coords)
    coords = line.coords

    pd = 0
    last = coords[0]
    for i, p in enumerate(coords):
        if i == 0:
            continue
        pd += _point_distance(last, p)

        if pd == distance:
            return [coords[: i + 1], coords[i:]]
        if pd > distance:
            cp = line.interpolate(distance)
            return coords[:i] + [(cp.x, cp.y)]

        last = p
    # If the code reaches this point, we've hit a floating point error or
    # something, as the total estimated distance traveled is less than the
    # distance specified and the distance specified is less than the length of
    # the geometry, so there's some small gap. The approach floating around
    # online is to use linear projection to find the closest point to the given
    # distance, but this is not robust against complex, self-intersection
    # lines. So, instead: we just assume it's between the second to last and
    # last point.
    cp = line.interpolate(distance)
    return coords[:i] + [(cp.x, cp.y)]


def _point_distance(p1, p2):
    """Distance between two points (l2 norm).

    :param p1: Point 1.
    :type p1: list of floats
    :param p2: Point 2.
    :type p2: list of floats

    """
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]

    return (dx ** 2 + dy ** 2) ** 0.5
