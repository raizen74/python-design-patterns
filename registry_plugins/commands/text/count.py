from rich import print

from registry import register_command


@register_command("text", "count")
def count_words(text: str) -> None:
    word_count = len(text.split())
    print(f"[green]Word count:[/green] [bold]{word_count}[/bold]")
