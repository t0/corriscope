"""YAML configuration loading with ``file:section`` selectors.

Replaces the private ``wtl.config``. ``load_yaml_config`` understands the selector
syntax used on the corriscope command lines and in ``fpga_master`` configs::

    [filename:]path {path | .relative.path | path=value} ...

- ``path`` is a dotted key path from the top of the YAML document. Every selected
  object is merged into the result (later ones win).
- A path starting with ``.`` is relative to the *parent* of the previously selected
  object: ``obj1.obj11 .obj12`` selects ``obj1.obj11`` then ``obj1.obj12``.
- ``path=value`` assigns a YAML-parsed value at ``path`` inside the result built so far.
- Fields are separated by spaces or ``;``. Quoted strings may contain separators.
"""
import logging
import re

import yaml

from .namespace import merge_dict

_FIELD_RE = re.compile(r"""(?:[^"'; ]|'[^']*(?:'|$)|"[^"]*(?:"|$))+""")


def load_yaml(stream):
    """Parse a YAML document (safe loader)."""
    return yaml.load(stream, Loader=yaml.SafeLoader)


def _split_fields(string):
    return [f.strip() for f in _FIELD_RE.findall(string)]


def _resolve(root, path):
    """Return the object at dotted ``path`` inside ``root``."""
    obj = root
    for key in path.split('.'):
        if not key:
            continue
        if isinstance(obj, list):
            key = int(key)
        obj = obj[key]
    return obj


def _absolute(path, last_path):
    """Turn a ``.relative`` path into an absolute one based on ``last_path``."""
    if not path.startswith('.'):
        return path
    base = last_path or ''
    while path.startswith('.'):
        base = base.rsplit('.', 1)[0] if '.' in base else ''
        path = path[1:]
    return f'{base}.{path}' if base else path


def load_yaml_config(object_names, default_filename='config.yaml'):
    """Load and merge the selected objects of a YAML file. Returns a plain dict.

    ``object_names`` is the selector string described in the module docstring (or a list
    of strings that are joined with spaces). Falsy input returns ``{}``.
    """
    if not object_names:
        return {}
    if not isinstance(object_names, str):
        object_names = ' '.join(object_names)
    fields = _split_fields(object_names)
    if not fields:
        return {}

    # Only the first field may carry a "filename:" prefix
    filename, sep, first = fields[0].rpartition(':')
    if sep:
        fields[0] = first
    else:
        filename = default_filename
    if any(':' in f.split('=')[0] for f in fields[1:]):
        raise ValueError('Only one filename can be specified')

    logging.getLogger(__name__).info(f'Loading YAML file {filename}')
    with open(filename, 'r', encoding='utf-8') as fh:
        document = load_yaml(fh) or {}

    config = {}
    last_yaml_path = None
    last_config_path = None
    for field in fields:
        if not field:
            continue
        name, eq, value = field.partition('=')
        if eq:
            if not value:
                raise ValueError(f'Incomplete assignment: {field!r}')
            path = _absolute(name, last_config_path)
            parent_path, _, key = path.rpartition('.')
            _resolve(config, parent_path)[key] = load_yaml(value)
            last_config_path = path
        else:
            path = _absolute(name, last_yaml_path)
            try:
                obj = _resolve(document, path)
            except (KeyError, IndexError, TypeError):
                raise AttributeError(f'Cannot find {path!r} in {filename}') from None
            config = merge_dict(config, obj)
            last_yaml_path = path
    return config
