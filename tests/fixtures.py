import os

import pytest

from entwiner import DiGraphDB, DiGraphDBView


from .constants import FEATURES

# TODO: Use a dynamic path and embed this into core graph functionality, like a
#       classmethod

GRAPH_PATH = "/tmp/test_graph.gpkg"


@pytest.fixture()
def G_test():
    if os.path.exists(GRAPH_PATH):
        os.remove(GRAPH_PATH)
    G = DiGraphDB.create_graph(path=GRAPH_PATH)

    # TODO: test integer node IDs as well as coordinates
    edges = []
    for feature in FEATURES["features"]:
        start_x = round(feature["geometry"]["coordinates"][0][0], 7)
        start_y = round(feature["geometry"]["coordinates"][0][1], 7)
        end_x = round(feature["geometry"]["coordinates"][-1][0], 7)
        end_y = round(feature["geometry"]["coordinates"][-1][1], 7)

        u = f"{start_x}, {start_y}"
        v = f"{end_x}, {end_y}"

        d = {"geom": feature["geometry"]}

        edges.append((u, v, d))
        rev_coords = list(reversed(feature["geometry"]["coordinates"]))
        d_rev = {"geom": {"type": "LineString", "coordinates": rev_coords}}

        edges.append((v, u, d_rev))

    G.add_edges_from(edges)

    del G

    G = DiGraphDBView(path=GRAPH_PATH)

    return G


@pytest.fixture()
def G_test_writable():
    if os.path.exists(GRAPH_PATH):
        os.remove(GRAPH_PATH)
    G = DiGraphDB.create_graph(path=GRAPH_PATH)

    # TODO: test integer node IDs as well as coordinates
    edges = []
    for feature in FEATURES["features"]:
        start_x = round(feature["geometry"]["coordinates"][0][0], 7)
        start_y = round(feature["geometry"]["coordinates"][0][1], 7)
        end_x = round(feature["geometry"]["coordinates"][-1][0], 7)
        end_y = round(feature["geometry"]["coordinates"][-1][1], 7)

        u = f"{start_x}, {start_y}"
        v = f"{end_x}, {end_y}"

        d = {"geom": feature["geometry"]}

        edges.append((u, v, d))
        rev_coords = list(reversed(feature["geometry"]["coordinates"]))
        d_rev = {"geom": {"type": "LineString", "coordinates": rev_coords}}

        edges.append((v, u, d_rev))

    G.add_edges_from(edges)

    return G
