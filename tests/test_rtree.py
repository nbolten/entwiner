def test_add_drop_rtree(G_test):
    # First run: rtree might not exist or be incomplete - should still run
    G_test.network.edges.drop_rtree()
    G_test.network.edges.add_rtree()

    # First run: rtree should definitely exist - drop and recreate in ideal
    # case
    G_test.network.edges.drop_rtree()
    G_test.network.edges.add_rtree()
