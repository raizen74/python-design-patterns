"""
Usage.

    uv run main.py text count foo
    uv run main.py text reverse foo
    uv run main.py text shout foo
"""

import importlib
import pkgutil

import typer

from registry import get_registry

app = typer.Typer()


def load_text_commands() -> None:
    import commands.text

    for module_finder, module_name, ispkg in pkgutil.iter_modules(commands.text.__path__):
        importlib.import_module(f"commands.text.{module_name}")


def load_plugins() -> None:
    import plugins

    # module_finder: the finder object responsible for locating modules in that path. Here it's a FileFinder pointing to the plugins directory
    # ispkg: True if the entry is a package (a directory with __init__.py), False if it's a plain module (a .py file). shout.py is a file, so False.
    for module_finder, module_name, ispkg in pkgutil.iter_modules(plugins.__path__):
        importlib.import_module(f"plugins.{module_name}")


def register_with_typer() -> None:
    group_apps: dict[str, typer.Typer] = {}

    for group, name, func in get_registry():
        if group not in group_apps:
            group_apps[group] = typer.Typer()
            app.add_typer(group_apps[group], name=group)
        group_apps[group].command(name)(func)


def main() -> None:
    load_text_commands()
    load_plugins()
    register_with_typer()
    app()


if __name__ == "__main__":
    main()
