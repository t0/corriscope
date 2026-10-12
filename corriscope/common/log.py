"""Logging helpers (replaces the private ``wtl.log``).

``get_logger(*names)`` builds a hierarchical logger name from strings and/or objects
(an object contributes ``module.ClassName``). ``setup_logging`` applies a
``logging.config.dictConfig`` dictionary after remapping logger names from the package
name used in the config file to the package the code actually runs under, and returns
the handlers it created so ``stop_logging`` can remove them again.
"""
import copy
import logging
import logging.config


def get_logger(*names):
    """Return the logger named by joining ``names``; no names gives the root logger.

    Examples: ``get_logger(__name__)``, ``get_logger(self)``, ``get_logger(__name__, 'fn()')``.
    """
    def to_name(x):
        if isinstance(x, str):
            return x
        cls = x.__class__
        return f'{cls.__module__}.{cls.__name__}'

    if not names:
        return logging.getLogger()
    logger = logging.getLogger(to_name(names[0]))
    for name in names[1:]:
        logger = logger.getChild(to_name(name))
    logger.disabled = False
    return logger


def get_parent_logger(module_name):
    """Logger of the package containing ``module_name`` (root logger if there is none)."""
    return logging.getLogger(module_name.rsplit('.', 1)[0] if '.' in module_name else '')


def setup_logging(dict_config=None, log_levels=None, base_package_name=None,
                  script_name=None, actual_package_name=None, **kwargs):
    """Configure logging from a dictConfig-style dict and return the handlers added.

    Parameters:
        dict_config (dict): ``logging.config.dictConfig`` dictionary (``version`` is added
            if missing). Handler ``filename`` strings containing ``%(`` are formatted
            with ``kwargs`` (e.g. ``%(run_folder)s``).
        log_levels (dict): ``{handler_name: level}`` overrides applied to ``handlers``.
        base_package_name (str): package prefix used for logger names in the config
            (e.g. ``'corriscope'``). It is stripped and replaced by ``actual_package_name``.
        script_name (str): logger name whose config is duplicated under ``__main__`` so a
            module run as a script keeps its settings.
        actual_package_name (str): package the code really runs under.

    Returns:
        dict: ``{logger_name: [new handlers]}`` for :func:`stop_logging`.
    """
    cfg = copy.deepcopy(dict(dict_config or {}))
    cfg.setdefault('version', 1)
    handlers = cfg.setdefault('handlers', {})
    loggers = cfg.setdefault('loggers', {})

    for handler_name, level in (log_levels or {}).items():
        if handler_name in handlers:
            handlers[handler_name]['level'] = level
    for hcfg in handlers.values():
        if isinstance(hcfg.get('level'), str):
            hcfg['level'] = hcfg['level'].upper()
        filename = hcfg.get('filename')
        if isinstance(filename, str) and '%(' in filename:
            hcfg['filename'] = filename % kwargs
    for lcfg in loggers.values():
        if isinstance(lcfg.get('level'), str):
            lcfg['level'] = lcfg['level'].upper()

    if script_name in loggers:
        prefix, sep, _ = script_name.rpartition('.')
        loggers[prefix + sep + '__main__'] = loggers[script_name]

    remapped = {}
    for name, lcfg in loggers.items():
        new_name = name
        if base_package_name:
            if name == base_package_name:
                new_name = ''
            elif name.startswith(base_package_name + '.'):
                new_name = name[len(base_package_name) + 1:]
        if actual_package_name:
            new_name = actual_package_name + ('.' + new_name if new_name else '')
        remapped[new_name] = lcfg
    cfg['loggers'] = remapped

    old_handlers = {name: list(logging.getLogger(name).handlers) for name in remapped}
    logging.config.dictConfig(cfg)
    new_handlers = {}
    for name in remapped:
        logger = logging.getLogger(name)
        logger.disabled = False  # dictConfig disables loggers it does not know about
        new_handlers[name] = [h for h in logger.handlers if h not in old_handlers[name]]
    return new_handlers


def stop_logging(new_handlers):
    """Remove the handlers returned by :func:`setup_logging`."""
    for name, handlers in (new_handlers or {}).items():
        logger = logging.getLogger(name)
        for handler in handlers:
            logger.removeHandler(handler)


def setup_basic_logging(level='INFO'):
    """One stderr handler on the root logger with a short timestamped format."""
    setup_logging({
        'formatters': {'std': {'format': '%(asctime)s %(levelname)s %(name)s: %(message)s',
                               'datefmt': '%H:%M:%S'}},
        'handlers': {'stderr': {'class': 'logging.StreamHandler', 'formatter': 'std', 'level': level}},
        'loggers': {'': {'handlers': ['stderr'], 'level': level}},
    })
