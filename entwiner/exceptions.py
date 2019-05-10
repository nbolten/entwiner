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
