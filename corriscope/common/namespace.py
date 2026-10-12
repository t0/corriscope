"""Attribute-style access to nested mappings, plus a recursive dict merge.

Replaces the private ``wtl.namespace`` package. Only the surface corriscope uses is
provided: ``NameSpace`` (wrapping a dict or list, nested values come back wrapped),
``merge_dict``, ``is_mapping`` and ``is_sequence``.
"""
import collections.abc
import copy

import yaml


class NameSpace:
    """Wrap a mapping (or sequence) so its items can be read and written as attributes.

    ``ns.a.b`` is ``ns['a']['b']``; nested mappings are returned wrapped so the chain works.
    Attribute lookup for a missing key raises ``KeyError`` (item semantics); ``State`` in
    ``corriscope.common.state`` converts that to ``AttributeError``.
    """

    def __init__(self, *args, **kwargs):
        if not args:
            obj = kwargs
        elif len(args) > 1 or kwargs:
            raise TypeError('Specify either a single object or keyword arguments')
        elif isinstance(args[0], NameSpace):
            obj = args[0]._obj
        else:
            obj = args[0]
        object.__setattr__(self, '_obj', obj)

    @staticmethod
    def _to_namespace(value):
        return NameSpace(value) if isinstance(value, collections.abc.Mapping) else value

    # attribute access --------------------------------------------------------------
    def __getattr__(self, name):
        obj = object.__getattribute__(self, '_obj')
        try:
            return getattr(obj, name)
        except AttributeError:
            if hasattr(obj, '__getitem__'):
                return self._to_namespace(obj[name])
            raise

    def __setattr__(self, name, value):
        obj = self._obj
        if hasattr(obj, '__getitem__'):
            obj[name] = value
        else:
            setattr(obj, name, value)

    def __dir__(self):
        keys = list(self._obj.keys()) if isinstance(self._obj, collections.abc.Mapping) else []
        return sorted(set(dir(type(self)) + dir(self._obj) + keys))

    # item access -------------------------------------------------------------------
    def __getitem__(self, key):
        return self._to_namespace(self._obj[key])

    def __setitem__(self, key, value):
        self._obj[key] = value

    def __delitem__(self, key):
        del self._obj[key]

    def __iter__(self):
        for x in self._obj:
            yield self._to_namespace(x)

    def __len__(self):
        return len(self._obj)

    def __contains__(self, key):
        return key in self._obj

    def __bool__(self):
        return bool(self._obj)

    # No __eq__: two NameSpaces compare by identity (State's change tracking depends on it);
    # compare contents with ``ns.as_dict() == other.as_dict()``.

    def __str__(self):
        return str(self._obj)

    def __repr__(self):
        return f'NameSpace({self._obj!r})'

    # mapping helpers -----------------------------------------------------------------
    def keys(self):
        return self._obj.keys()

    def items(self):
        return [(k, self._to_namespace(v)) for k, v in self._obj.items()]

    def values(self):
        return [self._to_namespace(v) for v in self._obj.values()]

    def get(self, key, default=None):
        return self._to_namespace(self._obj.get(key, default))

    def pop(self, *args):
        return self._to_namespace(self._obj.pop(*args))

    def setdefault(self, key, default=None):
        return self._to_namespace(self._obj.setdefault(key, default))

    def copy(self):
        return NameSpace(copy.copy(self._obj))

    def deepcopy(self):
        return NameSpace(copy.deepcopy(self._obj))

    def as_dict(self):
        """Return a plain (deeply unwrapped) copy of the underlying object."""
        def unwrap(v):
            if isinstance(v, NameSpace):
                return unwrap(v._obj)
            if isinstance(v, collections.abc.Mapping):
                return {k: unwrap(i) for k, i in v.items()}
            if is_sequence(v):
                return [unwrap(i) for i in v]
            return v
        return unwrap(self._obj)

    def as_yaml(self):
        return yaml.safe_dump(self.as_dict(), default_flow_style=False)

    def merge(self, other):
        """Merge ``other`` into this namespace (``other`` wins on conflicts)."""
        object.__setattr__(self, '_obj', merge_dict(self._obj, other))
        return self

    def is_mapping(self):
        return isinstance(self._obj, collections.abc.Mapping)

    def is_sequence(self):
        return is_sequence(self._obj)


def is_mapping(obj):
    if isinstance(obj, NameSpace):
        obj = obj._obj
    return isinstance(obj, collections.abc.Mapping)


def is_sequence(obj):
    if isinstance(obj, NameSpace):
        obj = obj._obj
    return isinstance(obj, collections.abc.Sequence) and not isinstance(obj, (str, bytes))


def merge_dict(src, dest, skip_none=False, add_lists=False):
    """Return ``src`` updated recursively with ``dest``.

    Mappings merge key by key (``dest`` wins for leaves). A mapping can only be merged
    with a mapping. With ``add_lists`` two sequences are concatenated. Anything else is
    replaced by ``dest``. ``skip_none`` leaves ``src`` untouched when ``dest`` is None.
    """
    if skip_none and dest is None:
        return src
    if isinstance(src, NameSpace):
        src = src._obj
    if isinstance(dest, NameSpace):
        dest = dest._obj
    if is_mapping(src):
        if not is_mapping(dest):
            raise TypeError(f'Only a mapping can be merged with another mapping. '
                            f'Types are src={type(src)}, dest={type(dest)}')
        new = type(src)()
        for k in set(src) | set(dest):
            if k in src and k in dest:
                new[k] = merge_dict(src[k], dest[k], skip_none=skip_none, add_lists=add_lists)
            else:
                new[k] = src[k] if k in src else dest[k]
        return new
    if add_lists and is_sequence(src):
        if not is_sequence(dest):
            raise TypeError(f'Only a sequence can be merged with another sequence. '
                            f'Types are src={type(src)}, dest={type(dest)}')
        return list(src) + list(dest)
    return dest
