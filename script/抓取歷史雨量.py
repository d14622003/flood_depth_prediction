# -*- coding: utf-8 -*-
"""
Created on Mon May  4 21:03:58 2026

@author: Nitro
"""

#%% 整理github上面檔案
from pathlib import Path
import pandas as pd
import re

rain_folder = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "rainfall"
)

all_dfs = []

for file in sorted(rain_folder.glob("C0M650_*.csv")):
    print("reading:", file.name)

    df = pd.read_csv(
        file,
        encoding="utf-8-sig",
        low_memory=False
    )

    df.columns = df.columns.str.strip()

    # 從檔名抓年份，例如 C0M650_2019.csv -> 2019
    match = re.search(r"\d{4}", file.name)
    year = int(match.group()) if match else None

    # 整理欄位
    df = df.rename(columns={
        "Unnamed: 0": "timestamp",
        "Precp": "rain_1h"
    })

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce"
    )

    df["rain_1h"] = pd.to_numeric(
        df["rain_1h"],
        errors="coerce"
    )

    # 負值通常是缺值或異常值，不當作雨量
    df.loc[df["rain_1h"] < 0, "rain_1h"] = pd.NA

    df["rain_station_id"] = "C0M650"
    df["year"] = year
    df["source_file"] = file.name

    keep_cols = [
        "timestamp",
        "rain_station_id",
        "rain_1h",
        "year",
        "source_file"
    ]

    df = df[keep_cols]

    all_dfs.append(df)

rain_hourly_all = pd.concat(all_dfs, ignore_index=True)

rain_hourly_all = (
    rain_hourly_all
    .dropna(subset=["timestamp"])
    .drop_duplicates(subset=["timestamp", "rain_station_id"])
    .sort_values("timestamp")
    .reset_index(drop=True)
)

print(rain_hourly_all.head())
print(rain_hourly_all.tail())
print(rain_hourly_all.shape)
#%% 存檔
from pathlib import Path

output_file="../output/C0M650_歷史時雨量.csv"

rain_hourly_all.to_csv(
    output_file,
    index=False,
    encoding="utf-8-sig"
)

print("已輸出:", output_file)