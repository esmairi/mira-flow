"""Handler registry: plain named functions, no topic knowledge."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mira_flow.transport import Message
from mira_flow.wiring.config import ConfigError

Handler = Callable[[Message, dict[str, Any]], dict[str, Any] | None]


class HandlerRegistry:
    """Maps job handler names (from config) to the Python callables they run."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str, fn: Handler) -> None:
        """Registers a handler under a name.

        Args:
            name: The name jobs reference via `job.handler` in config.
            fn: The callable to register.

        Raises:
            ValueError: If `name` is already registered.
        """
        if name in self._handlers:
            raise ValueError(f"handler {name!r} is already registered")
        self._handlers[name] = fn

    def resolve(self, name: str) -> Handler:
        """Looks up a registered handler by name.

        Args:
            name: The handler name to resolve.

        Returns:
            The registered callable.

        Raises:
            KeyError: If no handler is registered under `name`.
        """
        try:
            return self._handlers[name]
        except KeyError:
            registered = sorted(self._handlers) or ["<none>"]
            raise KeyError(
                f"unknown handler {name!r}; registered handlers: {', '.join(registered)}"
            ) from None

    def handler(self, name: str) -> Callable[[Handler], Handler]:
        """Decorator that registers a function under a name.

        Args:
            name: The name to register the decorated function under.

        Returns:
            A decorator that registers its target and returns it unchanged.
        """

        def decorator(fn: Handler) -> Handler:
            self.register(name, fn)
            return fn

        return decorator


_default_registry = HandlerRegistry()


def default_registry() -> HandlerRegistry:
    """Returns the process-wide default HandlerRegistry."""
    return _default_registry


def handler(name: str) -> Callable[[Handler], Handler]:
    """Registers a function in the default registry: `@handler("my_name")`.

    Args:
        name: The name to register the decorated function under.

    Returns:
        A decorator that registers its target and returns it unchanged.
    """
    return _default_registry.handler(name)


def load_handlers_from_dir(path: str | Path) -> list[str]:
    """Imports every `.py` file in a directory, so its `@handler`s register.

    Each file is loaded directly by its filesystem path (`importlib.util`),
    not through the normal package-import machinery — so a handlers
    directory doesn't need an `__init__.py`, a dotted module name, or to
    live on `sys.path`. Just drop a `.py` file with `@handler("name")`-
    decorated functions in the directory (see `examples/echo.py`) and point
    `--handlers-dir`/`HANDLERS_DIR` at it; nothing here calls `register()`
    itself, importing the file is what runs its `@handler` decorators.

    Args:
        path: Directory to scan, recursively, for handler files. Files (and
            directories) starting with `_` are skipped — e.g. `__init__.py`,
            `__pycache__`.

    Returns:
        Paths of the files actually imported, in scan order — for logging.

    Raises:
        ConfigError: If `path` doesn't exist or isn't a directory.
    """
    directory = Path(path)
    if not directory.is_dir():
        raise ConfigError(f"handlers directory not found: {directory}")

    loaded: list[str] = []
    for file in sorted(directory.rglob("*.py")):
        if any(part.startswith("_") for part in file.relative_to(directory).parts):
            continue
        _import_file(file)
        loaded.append(str(file))
    return loaded


def _import_file(file: Path) -> None:
    """Imports a single `.py` file by path, independent of `sys.path`.

    Args:
        file: The file to import.
    """
    # A hash of the full path keeps the synthetic module name unique (two
    # files named handlers.py in different subdirectories don't collide)
    # without needing to sanitize arbitrary path characters into one.
    digest = hashlib.sha1(str(file.resolve()).encode()).hexdigest()[:10]
    module_name = f"_mira-flow_handlers.{file.stem}_{digest}"
    spec = importlib.util.spec_from_file_location(module_name, file)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
