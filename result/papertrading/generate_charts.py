import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter


CSV_PATH = "analytics_history.csv"
PASTEL_COLORS = ["#FFD1DC", "#AEC6CF", "#77DD77", "#FDFD96"]


def load_and_prepare_data(csv_path: str) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Load CSV, validate required columns, and compute helper series."""
    df = pd.read_csv(csv_path)

    required_columns = {
        "timestamp",
        "equity",
        "inventory",
        "price",
        "elapsed_seconds",
        "rsi",
        "adx",
    }
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if df["timestamp"].isna().all():
        raise ValueError("All timestamp values failed to parse. Expected ISO 8601 format.")

    # Keep rows with valid timestamp and elapsed_seconds, then sort by elapsed_seconds.
    df = df.dropna(subset=["timestamp", "elapsed_seconds"]).copy()
    df = df.sort_values("elapsed_seconds").reset_index(drop=True)

    if df.empty:
        raise ValueError("No valid rows available after parsing timestamp/elapsed_seconds.")

    initial_equity = float(df["equity"].iloc[0])
    if initial_equity == 0:
        raise ValueError("Initial equity is zero; HPR calculation would divide by zero.")

    df["hpr"] = (df["equity"] - initial_equity) / initial_equity
    rolling_max_equity = df["equity"].cummax()
    df["drawdown"] = (rolling_max_equity - df["equity"]) / rolling_max_equity

    df["date"] = df["timestamp"].dt.date
    spans = (
        df.groupby("date", as_index=False)["elapsed_seconds"]
        .agg(min_elapsed="min", max_elapsed="max")
        .sort_values("min_elapsed")
        .reset_index(drop=True)
    )

    return df, spans, initial_equity


def add_day_background_spans(ax: plt.Axes, spans: pd.DataFrame) -> None:
    """Shade each day range with alternating pastel colors."""
    for i, row in spans.iterrows():
        color = PASTEL_COLORS[i % len(PASTEL_COLORS)]
        ax.axvspan(row["min_elapsed"], row["max_elapsed"], color=color, alpha=0.22, zorder=0)


def save_line_chart(
    df: pd.DataFrame,
    spans: pd.DataFrame,
    y_col: str,
    title: str,
    y_label: str,
    output_file: str,
    percent_axis: bool = False,
    line_color: str = "#1f77b4",
) -> None:
    fig, ax = plt.subplots(figsize=(11, 4.8))

    add_day_background_spans(ax, spans)
    ax.plot(df["elapsed_seconds"], df[y_col], color=line_color, linewidth=1.8, zorder=2)

    ax.set_title(title)
    ax.set_xlabel("Elapsed Seconds")
    ax.set_ylabel(y_label)
    ax.grid(True, alpha=0.25)

    if percent_axis:
        ax.yaxis.set_major_formatter(PercentFormatter(1.0))

    fig.tight_layout()
    fig.savefig(output_file, format="svg")
    plt.close(fig)


def main() -> None:
    df, spans, _ = load_and_prepare_data(CSV_PATH)

    save_line_chart(
        df=df,
        spans=spans,
        y_col="inventory",
        title="Inventory Over Time",
        y_label="Inventory",
        output_file="inventory.svg",
        line_color="#205781",
    )

    save_line_chart(
        df=df,
        spans=spans,
        y_col="hpr",
        title="Holding Period Return (HPR) Over Time",
        y_label="HPR (%)",
        output_file="hpr.svg",
        percent_axis=True,
        line_color="#2A9D8F",
    )

    save_line_chart(
        df=df,
        spans=spans,
        y_col="price",
        title="Price Over Time",
        y_label="Price",
        output_file="price.svg",
        line_color="#E76F51",
    )

    save_line_chart(
        df=df,
        spans=spans,
        y_col="drawdown",
        title="Drawdown Over Time",
        y_label="Drawdown (%)",
        output_file="drawdown.svg",
        percent_axis=True,
        line_color="#6A4C93",
    )


if __name__ == "__main__":
    main()
