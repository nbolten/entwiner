TEST_NODE1 = "-122.313294, 47.6598762"
TEST_NODE2 = "-122.3141965, 47.659887"


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
