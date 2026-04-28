from rich import print

from registry import register_command


@register_command("text", "shout")
def shout(text: str) -> None:
    print(f"[bold red]{text.upper()}!!![/bold red]")
