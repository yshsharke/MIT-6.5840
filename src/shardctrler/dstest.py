import itertools
import math
import signal
import subprocess
import tempfile
import shutil
import time
import os
import sys
import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, DefaultDict, Tuple
from typing_extensions import Annotated

import typer
import rich
from rich import print
from rich.table import Table
from rich.progress import (
    Progress,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TextColumn,
    BarColumn,
    SpinnerColumn,
)
from rich.live import Live
from rich.panel import Panel
from rich.traceback import install

install(show_locals=True)

@dataclass
class StatsMeter:

    n: int = 0
    mean: float = 0.0
    S: float = 0.0

    def add(self, datum):
        self.n += 1
        delta = datum - self.mean
        # Mk = Mk-1+ (xk – Mk-1)/k
        self.mean += delta / self.n
        # Sk = Sk-1 + (xk – Mk-1)*(xk – Mk).
        self.S += delta * (datum - self.mean)

    @property
    def variance(self):
        return self.S / self.n

    @property
    def std(self):
        return math.sqrt(self.variance)

def print_results(results: Dict[str, Dict[str, StatsMeter]]):
    table = Table(show_header=True, header_style="bold")
    table.add_column("Test")
    table.add_column("Failed", justify="right")
    table.add_column("Total", justify="right")
    table.add_column("Time", justify="right")

    for test, stats in results.items():
        if stats["completed"].n == 0:
            continue
        color = "green" if stats["failed"].n == 0 else "red"
        row = [
            f"[{color}]{test}[/{color}]",
            str(stats["failed"].n),
            str(stats["completed"].n),
            f"{stats['time'].mean:.2f} ± {stats['time'].std:.2f}"
        ]
        table.add_row(*row)

    print(table)


def run_test(test: str, race: bool):
    test_cmd = ["go", "test", f"-run={test}"]
    if race:
        test_cmd.append("-race")
    f, path = tempfile.mkstemp()
    start = time.time()
    proc = subprocess.run(test_cmd, stdout=f, stderr=f)
    runtime = time.time() - start
    os.close(f)
    return test, path, proc.returncode, runtime

def main(
    tests: Annotated[List[str], typer.Argument(help="Tests to execute")],
    sequential: Annotated[bool, typer.Option('--sequential', '-s', help='Run all test of each group in order')] = False,
    workers: Annotated[int, typer.Option('--workers', '-w', help='Number of parallel tasks')] = 4,
    iterations: Annotated[int, typer.Option('--iter', '-n', help='Number of iterations to run')] = 10,
    output: Annotated[Optional[Path], typer.Option('--output', '-o', help='Output path to use')] = None,
    archive: Annotated[bool, typer.Option('--archive', '-a', help='Save all logs instead of only failed ones')] = False,
    race: Annotated[bool, typer.Option('--race/--no-race', '-r', help='Run with race checker')] = False,
    loop: Annotated[bool, typer.Option('--loop', '-l', help='Run continuously')] = False,
    growth: Annotated[int, typer.Option('--growth', '-g', help='Growth ratio of iterations when using --loop')] = 1,
):

    if output is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output = Path(timestamp)
    print(f'Output dir: {output}')

    if race:
        print("[yellow]Running with the race detector\n[/yellow]")

    while True:
        total = iterations * len(tests)
        completed = 0

        results = {test: defaultdict(StatsMeter) for test in tests}

        if sequential:
            test_instances = itertools.chain.from_iterable(itertools.repeat(test, iterations) for test in tests)
        else:
            test_instances = itertools.chain.from_iterable(itertools.repeat(tests, iterations))
        test_instances = iter(test_instances)

        total_progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        )
        total_task = total_progress.add_task("[yellow]Total[/yellow]", total=total)

        task_progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            SpinnerColumn(),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
        )
        tasks = {test: task_progress.add_task(test, total=iterations) for test in tests}

        progress_table = Table.grid()
        progress_table.add_row(total_progress)
        progress_table.add_row(Panel.fit(task_progress, title="Tests"))

        with Live(progress_table, transient=True) as live:

            def handler(_, frame):
                live.stop()
                print('\n')
                print_results(results)
                raise typer.Exit()

            signal.signal(signal.SIGINT, handler)

            with ThreadPoolExecutor(max_workers=workers) as executor:

                futures = []
                while completed < total:
                    n = len(futures)
                    if n < workers:
                        for test in itertools.islice(test_instances, workers-n):
                            futures.append(executor.submit(run_test, test, race))

                    done, not_done = wait(futures, return_when=FIRST_COMPLETED)

                    for future in done:
                        test, path, rc, runtime = future.result()

                        results[test]['completed'].add(1)
                        results[test]['time'].add(runtime)
                        task_progress.update(tasks[test], advance=1)
                        if rc != 0:
                            print(f"Failed test {test}")
                            task_progress.update(tasks[test], description=f"[red]{test}[/red]")
                            results[test]['failed'].add(1)
                        else:
                            if results[test]['completed'].n == iterations and results[test]['failed'].n == 0:
                                task_progress.update(tasks[test], description=f"[green]{test}[/green]")

                        if rc != 0 or archive:
                            output.mkdir(exist_ok=True, parents=True)
                            dest = (output / f"{test}_{completed}.log").as_posix()
                            shutil.copy(path, dest)

                        os.remove(path)

                        completed += 1
                        total_progress.update(total_task, advance=1)

                        futures = list(not_done)

        print_results(results)

        if loop:
            iterations *= growth
            print(f"[yellow]Increasing iterations to {iterations}[/yellow]")
        else:
            break

if __name__ == "__main__":
    typer.run(main)