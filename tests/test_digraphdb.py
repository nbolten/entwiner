import time


# Test geospatial data
TEST_NODE1 = "-122.313294, 47.6598762"
TEST_NODE2 = "-122.3141965, 47.659887"

# Profiling conditions: should be able to update 1000 edges in 0.5 seconds
N_UPDATES = 1000
MAXIMUM_UPDATE_TIME = 0.5


def test_update(G_test_writable):
    key = "weight"
    value = 5.4
    first_edge = next(iter(G_test_writable.edges(data=True)))
    u, v, d = first_edge
    d[key] = value
    assert d[key] == value


def test_update_fid(G_test_writable):
    key = "fid"
    value = 700
    first_edge = next(iter(G_test_writable.edges(data=True)))
    u, v, d = first_edge
    d[key] = value
    assert d[key] != value


def test_update_speed(G_test_writable):
    key = "weight"
    value = 5.4
    first_edge = next(iter(G_test_writable.edges(data=True)))
    u, v, d = first_edge
    ebunch = []
    for i in range(N_UPDATES):
        d2 = {**d, key: value}
        d2.pop("fid")
        ebunch.append((u, v, d2))
    before = time.time()
    G_test_writable.network.edges.update(ebunch)
    after = time.time()

    assert (after - before) < MAXIMUM_UPDATE_TIME
