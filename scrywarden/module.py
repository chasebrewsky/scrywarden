from importlib import import_module
from typing import Any


def import_string(path: str) -> Any:
    """Imports a dotted path name and returns the class/attribute.

    Parameters
    ----------
    path: str
        Dotted module path to retrieve.

    Returns
    -------
    Class/attribute at the given import path.

    Raises
    ------
    ImportError
        If the path does not exist.
    """
    try:
        module_path, class_name = path.rsplit('.', 1)
    except ValueError as error:
        raise ImportError(
            f"{path} does not look like a module path",
        ) from error
    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as error:
        raise ImportError(
            f"Module '{module_path}' does not define a '{class_name}' "
            "attribute/class",
        ) from error
