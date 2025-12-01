# analysis.py
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB_PATH = "heatgrid.duckdb"
FIG_DIR = Path("figures")
FIG_DIR.mkdir(exist_ok=True)

def load_data():
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT
            station,
            region,
            day_utc,
            daily_max_temp_C,
            avg_temp_C,
            is_hot_day,
            is_heatwave_day,
            daily_total_mwh,
            daily_peak_mwh
        FROM heat_load_daily
        ORDER BY region, day_utc;
    """).fetchdf()
    con.close()

    df["day_utc"] = pd.to_datetime(df["day_utc"])

    # Force numeric columns
    for col in ["daily_max_temp_C", "avg_temp_C",
                "daily_total_mwh", "daily_peak_mwh"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Create human-readable day type
    df["day_type"] = "Normal Day"
    df.loc[df["is_hot_day"] == 1, "day_type"] = "Hot Day"
    df.loc[df["is_heatwave_day"] == 1, "day_type"] = "Heatwave Day"

    return df


def plot_avg_load_by_daytype(df):
    sub = df.dropna(subset=["daily_total_mwh"]).copy()

    agg = (
        sub.groupby(["region", "day_type"], as_index=False)["daily_total_mwh"]
           .mean()
           .rename(columns={"daily_total_mwh": "avg_daily_mwh"})
    )

    print("\n[DEBUG] avg load by day type:")
    print(agg)

    if agg.empty:
        print("[WARN] nothing to plot for day types.")
        return

    pivot = agg.pivot(index="region", columns="day_type", values="avg_daily_mwh")

    ax = pivot.plot(kind="bar", figsize=(8, 5))
    ax.set_title("Average Daily Electricity Load: Normal vs Hot vs Heatwave")
    ax.set_xlabel("Region")
    ax.set_ylabel("Average Daily Load (MWh)")
    plt.tight_layout()

    out = FIG_DIR / "avg_load_daytypes.png"
    plt.savefig(out)
    plt.close()
    print(f"✓ Saved: {out}")


def plot_temp_vs_load_scatter(df, region):
    sub = df[(df["region"] == region) &
             df["daily_total_mwh"].notna()].copy()

    color_map = {
        "Normal Day": "blue",
        "Hot Day": "orange",
        "Heatwave Day": "red"
    }

    plt.figure(figsize=(8, 6))
    plt.scatter(
        sub["daily_max_temp_C"],
        sub["daily_total_mwh"],
        c=sub["day_type"].map(color_map),
        alpha=0.6
    )

    plt.title(f"{region}: Temperature vs Load (3 Types)")
    plt.xlabel("Daily max temp (°C)")
    plt.ylabel("Daily total load (MWh)")

    import matplotlib.lines as mlines
    legend = [
        mlines.Line2D([], [], color=color_map[k], marker="o", linestyle="None", label=k)
        for k in color_map
    ]
    plt.legend(handles=legend)

    plt.tight_layout()
    out = FIG_DIR / f"scatter_temp_vs_load_{region}.png"
    plt.savefig(out)
    plt.close()
    print(f"✓ Saved: {out}")


def plot_time_series_with_heatwaves(df, station):
    sub = df[df["station"] == station].copy()

    plt.figure(figsize=(12, 5))
    plt.plot(sub["day_utc"], sub["daily_total_mwh"],
             color="gray", label="Daily load")

    # Hot days (not heatwave)
    hot = sub[(sub["is_hot_day"] == 1) & (sub["is_heatwave_day"] == 0)]
    plt.scatter(hot["day_utc"], hot["daily_total_mwh"], color="orange",
                s=10, label="Hot Day")

    # Heatwave days
    hw = sub[sub["is_heatwave_day"] == 1]
    plt.scatter(hw["day_utc"], hw["daily_total_mwh"], color="red",
                s=20, label="Heatwave Day")

    plt.title(f"{station}: Daily Load with Hot Days + Heatwaves")
    plt.xlabel("Date")
    plt.ylabel("Daily Load (MWh)")
    plt.legend()

    plt.tight_layout()
    out = FIG_DIR / f"time_series_{station}.png"
    plt.savefig(out)
    plt.close()
    print(f"✓ Saved: {out}")

def main():
    print("Loading heat_load_daily...")
    df = load_data()

    print("Generating figures...")
    plot_avg_load_by_daytype(df)

    for region in ["PJM", "ISNE"]:
        plot_temp_vs_load_scatter(df, region)

    for station in ["IAD", "BOS"]:
        plot_time_series_with_heatwaves(df, station)

if __name__ == "__main__":
    main()
