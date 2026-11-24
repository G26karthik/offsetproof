"""Benchmark runner (Gate B+). Not implemented until Gates A/B are green."""

import click


@click.command()
@click.option("--config", required=True)
def main(config: str) -> None:
    raise SystemExit("NOT IMPLEMENTED: benchmark lands after Gate A verification")


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
