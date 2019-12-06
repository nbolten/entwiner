"""Entwiner build functions - create a graph and populate it from geospatial formats."""
import os

from . import io
from .graphs.digraphdb import DiGraphDB


def create_graph(
    paths, db_path, precision=7, batch_size=1000, changes_sign=None, counter=None
):
    """Create a DiGraphDB from input files.

    :param paths: list of file paths to use as inputs. Must be Fiona-compatible.
    :type paths: list of str
    :param db_path: path to the database to be created. Will overwrite if it already
                    exists.
    :type db_path: str
    :param precision: Rounding precision for considering endpoints to be joined at a
                      node - i.e. connected in the network. In same units as input
                      file(s). Default is intended for lon-lat (WGS84).
    :type precision: float
    :param batch_size: Number of features to be added per database call - speeds up
                       graph creation.
    :type batch_size: int

    """
    # TODO: consider creating a temporary DB and moving it after success rather than
    # backing up existing copy.
    if os.path.exists(db_path):
        os.rename(db_path, db_path + ".bak")

    G = DiGraphDB(path=db_path, create=True)

    for path in paths:
        edge_gen = io.edge_generator(
            path, precision, rev=True, changes_sign=changes_sign
        )
        G.add_edges_from(edge_gen, _batch_size=batch_size, counter=counter)

    G.reindex()

    if os.path.exists(db_path + ".bak"):
        os.remove(db_path + ".bak")

    return G
