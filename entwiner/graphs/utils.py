def sqlite_type(value):
    if type(value) == int:
        return "INTEGER"
    elif type(value) == float:
        return "REAL"
    else:
        return "TEXT"
