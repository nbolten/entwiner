TEST_NODE1 = "-122.313294, 47.6598762"
TEST_NODE2 = "-122.3141965, 47.659887"


def test_size(G_test):
    assert G_test.size() == 8


def test_iter_edges(G_test):
    iterator = G_test.iter_edges()
    # TODO: check output
    list(iterator)


def test_edges_dwithin(G_test):
    # FIXME: automatically create rtree indices for edge and node tables
    G_test.network.edges.add_rtree()
    # One of the nodes in the test case
    lon = -122.3132940
    lat = 47.6598762
    edges = G_test.edges_dwithin(lon, lat, 0.1, sort=True)
    # TODO: check output
    assert len(list(edges)) == 2


def test_to_in_memory(G_test):
    # TODO: check output, check all behaviors on in-memory database in its own
    #       test script
    G_test.to_in_memory()


def test_get_outer_succ(G_test):
    # TODO: check output more deeply
    succ = G_test[TEST_NODE1]
    assert dict(succ) == dict(G_test._adj[TEST_NODE1])
    assert dict(succ) == dict(G_test._succ[TEST_NODE1])


def test_get_outer_pred(G_test):
    # TODO: check output
    pred = set(G_test.predecessors(TEST_NODE1))
    assert pred == set(G_test._pred[TEST_NODE1].keys())


def test_get_inner_succ(G_test):
    # TODO: use more complex edges and check properties
    edge_data = G_test[TEST_NODE1][TEST_NODE2]
    edge_data = dict(edge_data)
    assert edge_data["_u"] == TEST_NODE1
    assert edge_data["_v"] == TEST_NODE2
    # TODO: inspect geom more carefully
    assert "geom" in edge_data
    assert edge_data["fid"] == 2
