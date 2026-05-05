# -*- coding: utf-8 -*-

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = (
    BASE_DIR
    / "data"
    / "processed"
    / "water_depth_by_station"
    / "station_74249b35-891e-479f-ad77-34c6e7434789_201903_202512.csv"
)


def add_water_table_level(hourly_df: pd.DataFrame) -> pd.DataFrame:
    result = hourly_df.copy()
    depth = result["hourly_water_depth_cm"]

    result["water table level"] = pd.NA
    result.loc[depth == 0, "water table level"] = 0
    result.loc[(depth > 0) & (depth < 1), "water table level"] = 1
    result.loc[(depth >= 1) & (depth < 10), "water table level"] = 2
    result.loc[(depth >= 10) & (depth < 50), "water table level"] = 3
    result.loc[depth >= 50, "water table level"] = 4
    result["water table level"] = result["water table level"].astype("Int64")

    return result


def load_station_data(csv_file: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_file, encoding="utf-8-sig", low_memory=False)
    df["timestamp_dt"] = pd.to_datetime(df["timestamp_dt"], errors="coerce")
    df["water_depth_cm"] = pd.to_numeric(df["water_depth_cm"], errors="coerce")
    df = df.dropna(subset=["timestamp_dt"]).copy()
    return df


def build_hourly_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    hourly_df = (
        df.groupby("timestamp_dt", as_index=False)["water_depth_cm"]
        .max()
        .set_index("timestamp_dt")
        .resample("1h")
        .max()
        .rename(columns={"water_depth_cm": "hourly_water_depth_cm"})
    )

    full_index = pd.date_range(
        start=hourly_df.index.min(),
        end=hourly_df.index.max(),
        freq="1h",
    )
    hourly_df = hourly_df.reindex(full_index)
    hourly_df.index.name = "timestamp"
    hourly_df = hourly_df.reset_index()

    return hourly_df


def summarize_flood_values(hourly_df: pd.DataFrame) -> pd.DataFrame:
    flooded = hourly_df.loc[hourly_df["hourly_water_depth_cm"] > 0, "hourly_water_depth_cm"]

    summary = {
        "筆數": int(flooded.count()),
        "平均值_cm": flooded.mean(),
        "最小值_cm": flooded.min(),
        "25%_cm": flooded.quantile(0.25),
        "50%_cm": flooded.quantile(0.50),
        "75%_cm": flooded.quantile(0.75),
        "最大值_cm": flooded.max(),
        "標準差_cm": flooded.std(),
    }

    return pd.DataFrame([summary])


raw_df = load_station_data(INPUT_FILE)
hourly_df = build_hourly_dataframe(raw_df)
hourly_df = add_water_table_level(hourly_df)
stats_df = summarize_flood_values(hourly_df)


def main() -> None:
    print("raw_df shape:", raw_df.shape)
    print("hourly_df shape:", hourly_df.shape)
    print("\nhourly_df head:")
    print(hourly_df.head(10).to_string(index=False))
    print("\nhourly_df tail:")
    print(hourly_df.tail(10).to_string(index=False))
    print("\n淹水深度統計:")
    print(stats_df.to_string(index=False))


if __name__ == "__main__":
    main()
#%%
output_file="../output/淹水深度.csv"
hourly_df.to_csv(
    output_file,
    index=False,
    encoding="utf-8-sig"
)

print("已輸出:", output_file)
