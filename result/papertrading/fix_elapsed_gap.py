import argparse
import csv
from pathlib import Path


def adjust_elapsed_seconds(
    input_path: Path,
    output_path: Path,
    start_line: int,
    shift_seconds: float,
    elapsed_column: str = "elapsed_seconds",
) -> tuple[int, int]:
    """Shift elapsed_seconds backward from start_line (1-based file line)."""
    if start_line < 2:
        raise ValueError("start_line must be >= 2 (line 1 is the CSV header).")

    rows_updated = 0
    total_rows = 0

    with input_path.open("r", newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("CSV appears to be empty.")
        if elapsed_column not in fieldnames:
            raise ValueError(f"Missing required column: {elapsed_column}")

        with output_path.open("w", newline="", encoding="utf-8") as dst:
            writer = csv.DictWriter(dst, fieldnames=fieldnames)
            writer.writeheader()

            # DictReader line_num maps to source file line numbers (header is line 1).
            for row in reader:
                total_rows += 1
                if reader.line_num >= start_line:
                    original_value = float(row[elapsed_column])
                    row[elapsed_column] = f"{original_value - shift_seconds}"
                    rows_updated += 1
                writer.writerow(row)

    return rows_updated, total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Subtract a fixed number of seconds from elapsed_seconds starting at a given file line."
        )
    )
    parser.add_argument(
        "--input",
        default="analytics_history.csv",
        help="Input CSV path (default: analytics_history.csv)",
    )
    parser.add_argument(
        "--output",
        default="analytics_history_adjusted.csv",
        help="Output CSV path (default: analytics_history_adjusted.csv)",
    )
    parser.add_argument(
        "--start-line",
        type=int,
        default=5967,
        help="1-based file line where adjustment starts (default: 5967)",
    )
    parser.add_argument(
        "--shift",
        type=float,
        default=5250.0,
        help="Seconds to subtract from elapsed_seconds (default: 5250)",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the input file instead of writing to --output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path = input_path if args.in_place else Path(args.output)

    if args.in_place:
        temp_path = input_path.with_suffix(input_path.suffix + ".tmp")
        rows_updated, total_rows = adjust_elapsed_seconds(
            input_path=input_path,
            output_path=temp_path,
            start_line=args.start_line,
            shift_seconds=args.shift,
        )
        temp_path.replace(input_path)
    else:
        rows_updated, total_rows = adjust_elapsed_seconds(
            input_path=input_path,
            output_path=output_path,
            start_line=args.start_line,
            shift_seconds=args.shift,
        )

    print(f"Updated {rows_updated} rows out of {total_rows} data rows.")
    print(f"Start line: {args.start_line}, shift applied: -{args.shift} seconds")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
