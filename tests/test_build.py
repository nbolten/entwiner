from entwiner import DiGraphDB
from entwiner.build import GraphBuilder


def test_build():
    builder = GraphBuilder(
        graph_class=DiGraphDB, precision=7, changes_sign=None
    )

    builder.create_temporary_db()
    builder.add_edges_from("./tests/data/uw.geojson")
    builder.finalize_db("/tmp/entwiner-throwaway.gpkg")
