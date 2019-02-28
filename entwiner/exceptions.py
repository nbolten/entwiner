"""Reusable package-level exceptions."""


class NodeNotFound(ValueError):
    pass


class EdgeNotFound(ValueError):
    pass


class UnknownGeometry(ValueError):
    pass


class ImmutableGraphError(Exception):
    pass
