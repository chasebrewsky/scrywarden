import typing as t

JSONValue = t.Union[str, int, float, bool, None, 'JSONDict', 'JSONList']
JSONDict = t.Dict[str, JSONValue]
JSONList = t.List[JSONValue]
