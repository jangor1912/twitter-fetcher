def remove_nones(item):
    if isinstance(item, dict):
        for key in item:
            item[key] = remove_nones(item[key])
    elif isinstance(item, list):
        for i, el in enumerate(item):
            item[i] = remove_nones(el)
    elif item is None:
        return "None"
    elif item == "":
        return "Empty"
    return item
