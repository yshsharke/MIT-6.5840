import sys
import typer
from typing import Optional, List
from typing_extensions import Annotated
from rich.columns import Columns
from rich.console import Console

TOPICS = {
    "TEST": "white",
    "CLNT": "bright_black",
    "SEVR": "cyan",
    "MACH": "orange_red1",
    "DBUG": "white",
    "CTRL": "green",
    "DCFG": "orange_red1",

    "TIMR": "bright_black",
    "VOTE": "cyan",
    "LEAD": "green",
    "FOLL": "yellow",
    "TERM": "orange_red1",
    "APPD": "bright_blue",
    "KILL": "red",
    "APPL": "purple",
    "PSST": "wheat4",
    "SNAP": "hot_pink",
}

def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics

def main(
    file: Annotated[typer.FileText, typer.Argument(help="File to read, stdin otherwise")] = None,
    colorize: Annotated[bool, typer.Option("--color/--no-color", help="Colorize output")] = True,
    n_columns: Annotated[Optional[int], typer.Option("--columns", "-c", help="Number of Columns of output")] = None,
    ignore: Annotated[Optional[str], typer.Option("--ignore", "-i", callback=list_topics, help="Topics hidden in output")] = None,
    just: Annotated[Optional[str], typer.Option("--just", "-j", callback=list_topics, help="Topics shown in output")] = None,
    n_groups: Annotated[Optional[int], typer.Option("--groups", "-g", help="Number of Groups")] = None,
):
    topics = list(TOPICS)

    n_columns+=1

    input_ = file if file else sys.stdin
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        try:
            time, topic, *msg = line.strip().split(" ")
            if topic not in topics:
                continue

            msg = " ".join(msg)

            if topic == "CTRL":
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[n_columns-1] = msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                console.print(cols)
                continue

            g = int(msg[1])
            i = int(msg[3])

            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"

            if n_columns is None:
                console.print(time, msg)
            else:
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[int(g * ((n_columns - 1) / n_groups) + i)] = msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                console.print(cols)
        except:
            if line.startswith("panic"):
                panic = True
            if not panic:
                console.print("#" * console.width)
            console.print(line, end="")

if __name__ == "__main__":
    typer.run(main)