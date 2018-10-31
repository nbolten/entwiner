def sqlite_type(value):
    if type(value) == int:
        return "integer"
    elif type(value) == float:
        return "real"
    else:
        return "text"
