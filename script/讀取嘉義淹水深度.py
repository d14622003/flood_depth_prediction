# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

STATION_FILE = DATA_DIR / "station_水利署_淹水感測器.csv"
RAW_DEPTH_ROOT = DATA_DIR / "歷年淹水感測資料"
START_YYYYMM = "201903"
END_YYYYMM = "202512"

OUT_MONTHLY_DIR = DATA_DIR / "processed" / "water_depth_monthly"
OUT_STATION_DIR = DATA_DIR / "processed" / "water_depth_by_station"
OUT_SUMMARY_DIR = DATA_DIR / "processed" / "summaries"

OUT_MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
OUT_STATION_DIR.mkdir(parents=True, exist_ok=True)
OUT_SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

TIMESTAMP_WITH_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$")
TIMESTAMP_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def read_chiayi_stations(station_file: Path) -> pd.DataFrame:
    stations = pd.read_csv(station_file, encoding="utf-8-sig", low_memory=False)
    stations.columns = stations.columns.str.strip()

    stations["station_name"] = stations["station_name"].astype("string").str.strip()
    stations["Address"] = stations["Address"].astype("string").str.strip()
    stations["PQ_name"] = stations["PQ_name"].astype("string").str.strip()
    stations["station_id"] = stations["station_id"].astype("string").str.strip()

    is_chiayi = (
        stations["station_name"].str.contains("嘉義", na=False)
        | stations["Address"].str.contains("嘉義", na=False)
    )

    stations = stations[(stations["PQ_name"] == "淹水深度") & is_chiayi].copy()
    stations = stations.drop_duplicates(subset=["station_id"]).copy()

    keep_cols = [
        "station_id",
        "station_name",
        "Address",
        "Longitude",
        "Latitude",
    ]
    return stations[keep_cols]


def month_folders(raw_depth_root: Path) -> list[Path]:
    return sorted(
        folder
        for folder in raw_depth_root.iterdir()
        if folder.is_dir() and re.fullmatch(r"\d{6}", folder.name)
    )


def month_in_range(yyyymm: str, start_yyyymm: str, end_yyyymm: str) -> bool:
    return start_yyyymm <= yyyymm <= end_yyyymm


def write_csv_with_fallback(df: pd.DataFrame, output_file: Path, label: str) -> Path:
    try:
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        return output_file
    except PermissionError:
        fallback_file = output_file.with_name(f"{output_file.stem}_201903_202512{output_file.suffix}")
        print(f"{label} locked, writing fallback file:", fallback_file.name)
        df.to_csv(fallback_file, index=False, encoding="utf-8-sig")
        return fallback_file


def normalize_timestamp(timestamp_series: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    timestamp_raw = timestamp_series.astype("string").str.strip()
    timestamp_dt = pd.to_datetime(timestamp_raw, errors="coerce", format="mixed")

    timestamp_text = pd.Series(pd.NA, index=timestamp_raw.index, dtype="string")
    parsed_mask = timestamp_dt.notna()
    timestamp_text.loc[parsed_mask] = timestamp_dt.loc[parsed_mask].dt.strftime("%Y-%m-%d %H:%M:%S")

    date_only_mask = timestamp_raw.str.fullmatch(TIMESTAMP_DATE_ONLY.pattern, na=False)
    timestamp_text.loc[date_only_mask & parsed_mask] = timestamp_dt.loc[date_only_mask & parsed_mask].dt.strftime(
        "%Y-%m-%d 00:00:00"
    )

    return timestamp_raw, timestamp_text, timestamp_dt


def process_one_month(month_folder: Path, station_ids: set[str], station_info: pd.DataFrame) -> pd.DataFrame:
    yyyymm = month_folder.name
    output_file = OUT_MONTHLY_DIR / f"chiayi_water_depth_{yyyymm}.csv"

    if output_file.exists():
        existing = pd.read_csv(output_file, encoding="utf-8-sig", low_memory=False)
        if {"station_id", "timestamp", "timestamp_raw", "timestamp_dt"}.issubset(existing.columns):
            print("monthly exists, reuse:", output_file.name)
            return existing

    zip_files = sorted(month_folder.glob("*.zip"))
    if not zip_files:
        print("no zip files, skip:", month_folder)
        return pd.DataFrame()

    all_dfs: list[pd.DataFrame] = []

    for zip_path in zip_files:
        print("reading:", zip_path.name)

        df = pd.read_csv(
            zip_path,
            compression="zip",
            encoding="utf-8-sig",
            dtype={"timestamp": "string", "station_id": "string"},
            low_memory=False,
        )
        df.columns = df.columns.str.strip()

        required_cols = {"station_id", "PQ_name", "PQ_unit", "timestamp", "value"}
        missing = required_cols - set(df.columns)
        if missing:
            print(f"skip {zip_path.name}, missing columns: {missing}")
            continue

        df["station_id"] = df["station_id"].astype("string").str.strip()
        df["PQ_name"] = df["PQ_name"].astype("string").str.strip()
        df["PQ_unit"] = df["PQ_unit"].astype("string").str.strip()

        df = df[(df["station_id"].isin(station_ids)) & (df["PQ_name"] == "淹水深度")].copy()
        if df.empty:
            continue

        timestamp_raw, timestamp_text, timestamp_dt = normalize_timestamp(df["timestamp"])
        df["timestamp_raw"] = timestamp_raw
        df["timestamp"] = timestamp_text
        df["timestamp_dt"] = timestamp_dt
        df["timestamp_has_time"] = timestamp_raw.str.fullmatch(TIMESTAMP_WITH_TIME.pattern, na=False)
        df["timestamp_date_only"] = timestamp_raw.str.fullmatch(TIMESTAMP_DATE_ONLY.pattern, na=False)

        df["value_raw"] = df["value"]
        df["water_depth_raw"] = pd.to_numeric(df["value"], errors="coerce")
        df["water_depth_nonneg"] = df["water_depth_raw"].clip(lower=0)

        df["water_depth_cm"] = df["water_depth_nonneg"]
        mm_mask = df["PQ_unit"].str.lower().eq("mm")
        df.loc[mm_mask, "water_depth_cm"] = df.loc[mm_mask, "water_depth_nonneg"] / 10

        match = re.search(r"\d{8}", zip_path.name)
        df["source_date"] = pd.to_datetime(match.group(), format="%Y%m%d") if match else pd.NaT
        df["source_file"] = zip_path.name

        keep_cols = [
            "station_id",
            "timestamp",
            "timestamp_raw",
            "timestamp_dt",
            "timestamp_has_time",
            "timestamp_date_only",
            "source_date",
            "PQ_name",
            "PQ_unit",
            "value_raw",
            "water_depth_raw",
            "water_depth_nonneg",
            "water_depth_cm",
            "source_file",
        ]
        all_dfs.append(df[keep_cols])

    if not all_dfs:
        print("no valid data in month:", yyyymm)
        return pd.DataFrame()

    month_df = pd.concat(all_dfs, ignore_index=True)
    month_df = month_df.merge(station_info, on="station_id", how="left")
    month_df = month_df.sort_values(["station_id", "timestamp_dt", "timestamp_raw"], na_position="last")

    month_df["timestamp_dt"] = month_df["timestamp_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
    month_df["source_date"] = pd.to_datetime(month_df["source_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    saved_file = write_csv_with_fallback(month_df, output_file, "monthly file")
    print("saved monthly:", saved_file.name, month_df.shape)
    return month_df


def process_all_months(start_yyyymm: str = START_YYYYMM, end_yyyymm: str = END_YYYYMM) -> None:
    station_info = read_chiayi_stations(STATION_FILE)
    station_ids = set(station_info["station_id"].dropna().astype("string"))

    print("chiayi flood-depth stations:", len(station_ids))
    print("month range:", start_yyyymm, "to", end_yyyymm)
    if not station_ids:
        print("no chiayi stations found in station file")
        return

    for folder in month_folders(RAW_DEPTH_ROOT):
        if not month_in_range(folder.name, start_yyyymm, end_yyyymm):
            continue
        print("=" * 60)
        print("processing:", folder.name)
        process_one_month(folder, station_ids, station_info)


def split_monthly_to_station_files(
    allowed_station_ids: set[str],
    start_yyyymm: str = START_YYYYMM,
    end_yyyymm: str = END_YYYYMM,
) -> None:
    monthly_files = sorted(OUT_MONTHLY_DIR.glob("chiayi_water_depth_*.csv"))
    if not monthly_files:
        print("no monthly csv files found")
        return

    station_groups: dict[str, list[pd.DataFrame]] = {}

    for file in monthly_files:
        month_match = re.search(r"(\d{6})", file.stem)
        if not month_match or not month_in_range(month_match.group(1), start_yyyymm, end_yyyymm):
            continue
        print("loading monthly:", file.name)
        df = pd.read_csv(file, encoding="utf-8-sig", dtype={"station_id": "string", "timestamp": "string"}, low_memory=False)
        if df.empty:
            continue

        df.columns = df.columns.str.strip()
        df["station_id"] = df["station_id"].astype("string").str.strip()
        df = df[df["station_id"].isin(allowed_station_ids)].copy()
        if df.empty:
            continue

        if "timestamp_raw" not in df.columns:
            if "timestamp" in df.columns:
                df["timestamp_raw"] = df["timestamp"].astype("string")
            else:
                df["timestamp_raw"] = pd.Series(pd.NA, index=df.index, dtype="string")

        if "timestamp_dt" not in df.columns:
            timestamp_raw, timestamp_text, timestamp_dt = normalize_timestamp(df["timestamp_raw"])
            df["timestamp_raw"] = timestamp_raw
            df["timestamp"] = timestamp_text
            df["timestamp_dt"] = timestamp_dt
        else:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp_dt"], errors="coerce")

        if "timestamp_date_only" not in df.columns:
            df["timestamp_date_only"] = df["timestamp_raw"].astype("string").str.fullmatch(
                TIMESTAMP_DATE_ONLY.pattern,
                na=False,
            )

        for station_id, group in df.groupby("station_id"):
            station_groups.setdefault(str(station_id), []).append(group)

    for station_id, groups in station_groups.items():
        station_df = pd.concat(groups, ignore_index=True)
        station_df = station_df.sort_values(["timestamp_dt", "timestamp_raw"], na_position="last")
        station_df["timestamp_dt"] = station_df["timestamp_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")

        output_file = OUT_STATION_DIR / f"station_{station_id}.csv"
        saved_file = write_csv_with_fallback(station_df, output_file, "station file")
        print("saved station:", saved_file.name, station_df.shape)


def summarize_station_file(file: Path) -> dict | None:
    df = pd.read_csv(file, encoding="utf-8-sig", low_memory=False)
    if df.empty:
        return None

    df.columns = df.columns.str.strip()

    if "timestamp_raw" not in df.columns:
        if "timestamp" in df.columns:
            df["timestamp_raw"] = df["timestamp"].astype("string")
        else:
            df["timestamp_raw"] = pd.Series(pd.NA, index=df.index, dtype="string")

    if "timestamp_dt" not in df.columns:
        timestamp_raw, timestamp_text, timestamp_dt = normalize_timestamp(df["timestamp_raw"])
        df["timestamp_raw"] = timestamp_raw
        if "timestamp" not in df.columns:
            df["timestamp"] = timestamp_text
        df["timestamp_dt"] = timestamp_dt
    else:
        df["timestamp_dt"] = pd.to_datetime(df["timestamp_dt"], errors="coerce")

    if "timestamp_date_only" not in df.columns:
        df["timestamp_date_only"] = df["timestamp_raw"].astype("string").str.fullmatch(
            TIMESTAMP_DATE_ONLY.pattern,
            na=False,
        )
    df["water_depth_raw"] = pd.to_numeric(df["water_depth_raw"], errors="coerce")
    df["water_depth_nonneg"] = pd.to_numeric(df["water_depth_nonneg"], errors="coerce")
    df["water_depth_cm"] = pd.to_numeric(df["water_depth_cm"], errors="coerce")
    df = df.sort_values("timestamp_dt")

    station_id = df["station_id"].iloc[0]
    station_name = df["station_name"].dropna().iloc[0] if "station_name" in df.columns and df["station_name"].notna().any() else None

    time_diff_min = df["timestamp_dt"].diff().dt.total_seconds().div(60)

    return {
        "station_id": station_id,
        "station_name": station_name,
        "n_records": len(df),
        "start_time": df["timestamp_dt"].min(),
        "end_time": df["timestamp_dt"].max(),
        "missing_timestamp_count": df["timestamp_dt"].isna().sum(),
        "date_only_timestamp_count": pd.Series(df["timestamp_date_only"]).fillna(False).sum(),
        "unit_values": ",".join(df["PQ_unit"].dropna().astype(str).unique()),
        "missing_value_count": df["water_depth_raw"].isna().sum(),
        "negative_count": (df["water_depth_raw"] < 0).sum(),
        "zero_count": (df["water_depth_nonneg"] == 0).sum(),
        "positive_count": (df["water_depth_nonneg"] > 0).sum(),
        "max_depth_raw": df["water_depth_raw"].max(),
        "p95_depth_raw": df["water_depth_raw"].quantile(0.95),
        "p99_depth_raw": df["water_depth_raw"].quantile(0.99),
        "max_depth_cm": df["water_depth_cm"].max(),
        "p95_depth_cm": df["water_depth_cm"].quantile(0.95),
        "p99_depth_cm": df["water_depth_cm"].quantile(0.99),
        "count_ge_1cm": (df["water_depth_cm"] >= 1).sum(),
        "count_ge_3cm": (df["water_depth_cm"] >= 3).sum(),
        "count_ge_5cm": (df["water_depth_cm"] >= 5).sum(),
        "count_ge_10cm": (df["water_depth_cm"] >= 10).sum(),
        "median_interval_min": time_diff_min.median(),
        "mode_interval_min": time_diff_min.mode().iloc[0] if not time_diff_min.mode().empty else None,
        "p90_interval_min": time_diff_min.quantile(0.90),
        "p99_interval_min": time_diff_min.quantile(0.99),
    }


def build_station_summary(allowed_station_ids: set[str]) -> pd.DataFrame:
    rows = []
    station_files = sorted(OUT_STATION_DIR.glob("station_*.csv"))
    if not station_files:
        print("no station csv files found")
        return pd.DataFrame()

    for file in station_files:
        try:
            station_id_from_name = file.stem.replace("station_", "", 1)
            if station_id_from_name not in allowed_station_ids:
                continue
            result = summarize_station_file(file)
            if result is not None:
                rows.append(result)
        except Exception as exc:
            print("summary failed:", file.name, exc)

    summary_df = pd.DataFrame(rows)
    output_file = OUT_SUMMARY_DIR / "station_depth_summary.csv"
    saved_file = write_csv_with_fallback(summary_df, output_file, "summary file")
    print("saved summary:", saved_file)
    print("summary shape:", summary_df.shape)
    return summary_df


def main() -> None:
    station_info = read_chiayi_stations(STATION_FILE)
    allowed_station_ids = set(station_info["station_id"].dropna().astype("string"))
    process_all_months(START_YYYYMM, END_YYYYMM)
    split_monthly_to_station_files(allowed_station_ids, START_YYYYMM, END_YYYYMM)
    station_summary = build_station_summary(allowed_station_ids)
    if not station_summary.empty:
        print(station_summary.head())


if __name__ == "__main__":
    main()
