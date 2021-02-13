import time

from entwiner import DiGraphDB
from entwiner.build import GraphBuilder


# Basic profiling condition: require that build time for about 10,000 inserts
# takes no more than 10 seconds. There are 112 edges in the University of
# Washington neighborhood dataset, so 100 inserts should be about right.
N_INSERTS = 100
MAXIMUM_BUILD_TIME = 10


def test_build():
    builder = GraphBuilder(
        graph_class=DiGraphDB, precision=7, changes_sign=None
    )

    builder.create_temporary_db()
    builder.add_edges_from("./tests/data/uw.geojson")
    builder.finalize_db("/tmp/entwiner-throwaway.gpkg")


def test_insert_time():
    builder = GraphBuilder(
        graph_class=DiGraphDB, precision=7, changes_sign=None
    )

    builder.create_temporary_db()
    before = time.time()
    for i in range(N_INSERTS):
        builder.add_edges_from("./tests/data/uw.geojson")
    after = time.time()
    print(after - before)
    assert (after - before) < MAXIMUM_BUILD_TIME
