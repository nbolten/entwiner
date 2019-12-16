"""Reusable package-level exceptions."""


class UnrecognizedFileFormat(ValueError):
    pass


class NodeNotFound(ValueError):
    pass


class EdgeNotFound(ValueError):
    pass


class UnknownGeometry(ValueError):
    pass


class ImmutableGraphError(Exception):
    pass


class UninitializedEdgeError(Exception):
    pass


class UnderspecifiedGraphError(Exception):
    def __init__(*args, **kwargs):
        default_message = "SQLiteGraph or path to database must be supplied. To create a new graph, use DiGraphDB.create_graph"

        if args or kwargs:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(default_message)
