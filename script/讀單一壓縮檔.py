# 讀取單一zip檔
from pathlib import Path

import pandas as pd


zip_path = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "歷年淹水感測資料"
    / "201811"
    / "wra_iow_水利署_淹水感測器_20181103.zip"
)

df = pd.read_csv(zip_path, compression="zip", encoding="utf-8-sig")

print("shape:", df.shape)
print("columns:", list(df.columns))
print(df.head())
#%% 取淹水深度測站資料
import pandas as pd
folder = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "station_水利署_淹水感測器.csv"
)
df = pd.read_csv(
    folder,
    encoding="utf-8-sig"
)

flood_station_list = (
    df[df["PQ_name"] == "淹水深度"]
      .drop_duplicates(subset=["station_id"])
      [[
          "station_id",
          "station_name",
          "OrgName",
          "Code",
          "Address",
          "Longitude",
          "Latitude",
          "PQ_id",
          "PQ_name",
          "SIUnit"
      ]]
)

flood_station_list
#%% 讀取嘉義_淹水深度測站資料
import pandas as pd
folder = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "station_水利署_淹水感測器.csv"
)
df = pd.read_csv(
    folder,
    encoding="utf-8-sig"
)

# 清理欄位名稱，避免 SIUnit 後面有空白
df.columns = df.columns.str.strip()

# 只取真正的淹水感測資料
flood = df[df["PQ_name"] == "淹水深度"].copy()

# 從 station_name 抽出行政區代碼，例如 108Q023 -> Q
flood["area_code"] = flood["station_name"].astype(str).str.extract(r"^\d{3}([A-Z])")

# 嘉義縣 Q + 嘉義市 I
chiayi = flood[flood["area_code"].isin(["Q", "I"])].copy()

# 每個站點只留一筆
chiayi_stations = chiayi.drop_duplicates(subset=["station_id"]).copy()

# 保留需要欄位
chiayi_stations = chiayi_stations[[
    "station_id",
    "station_name",
    "area_code",
    "OrgName",
    "Code",
    "Address",
    "Longitude",
    "Latitude",
    "PQ_id",
    "PQ_name",
    "SIUnit"
]]

chiayi_stations
#%%
# 嘉義測站 ID 清單
chiayi_station_ids = (
    chiayi_stations["station_id"]
    .astype(str)
    .str.strip()
    .unique()
)

print("嘉義測站數量:", len(chiayi_station_ids))
print(chiayi_station_ids[:10])
#%%
from pathlib import Path
import pandas as pd
import re

folder = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "歷年淹水感測資料"
    / "202001"
)

all_dfs = []

for zip_path in sorted(folder.glob("*.zip")):
    print("reading:", zip_path.name)

    df = pd.read_csv(
        zip_path,
        compression="zip",
        encoding="utf-8-sig",
        low_memory=False
    )

    df.columns = df.columns.str.strip()

    df["station_id"] = df["station_id"].astype(str).str.strip()

    # 只取嘉義測站
    df = df[df["station_id"].isin(chiayi_station_ids)].copy()

    # 只取淹水深度
    df = df[df["PQ_name"] == "淹水深度"].copy()

    if df.empty:
        continue

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    match = re.search(r"\d{8}", zip_path.name)

    if match:
        df["date"] = pd.to_datetime(match.group(), format="%Y%m%d")
    else:
        df["date"] = pd.NaT

    df["source_file"] = zip_path.name

    all_dfs.append(df)

# 合併所有嘉義測站水深資料
chiayi_depth_df = pd.concat(all_dfs, ignore_index=True)

print("嘉義水深資料筆數:", chiayi_depth_df.shape)
print(chiayi_depth_df.head())
#%% 讀取嘉義淹水深度資料：整合版

from pathlib import Path
import pandas as pd
import re


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

STATION_FILE = DATA_DIR / "station_水利署_淹水感測器.csv"
DEPTH_FOLDER = DATA_DIR / "歷年淹水感測資料" / "202001"


def read_chiayi_stations(station_file):
    """讀取測站清單，篩出嘉義縣市的淹水深度測站"""

    df = pd.read_csv(
        station_file,
        encoding="utf-8-sig",
        low_memory=False
    )

    df.columns = df.columns.str.strip()

    # 只取淹水深度測站
    df = df[df["PQ_name"] == "淹水深度"].copy()

    # 從 station_name 抽出行政區代碼，例如 108Q023 -> Q
    df["area_code"] = df["station_name"].astype(str).str.extract(r"^\d{3}([A-Z])")

    # 嘉義縣 Q + 嘉義市 I
    df = df[df["area_code"].isin(["Q", "I"])].copy()

    # 每個站點只留一筆
    df = df.drop_duplicates(subset=["station_id"]).copy()

    # station_id 統一成字串
    df["station_id"] = df["station_id"].astype(str).str.strip()

    keep_cols = [
        "station_id",
        "station_name",
        "area_code",
        "OrgName",
        "Code",
        "Address",
        "Longitude",
        "Latitude",
        "PQ_id",
        "PQ_name",
        "SIUnit"
    ]

    return df[keep_cols]


def read_depth_data(depth_folder, station_ids):
    """讀取 zip 水深資料，只保留指定 station_id 與淹水深度"""

    all_dfs = []

    for zip_path in sorted(depth_folder.glob("*.zip")):
        print("reading:", zip_path.name)

        df = pd.read_csv(
            zip_path,
            compression="zip",
            encoding="utf-8-sig",
            low_memory=False
        )

        df.columns = df.columns.str.strip()

        # 避免欄位不存在時直接報錯
        required_cols = {"station_id", "PQ_name", "value", "timestamp"}
        missing_cols = required_cols - set(df.columns)

        if missing_cols:
            print(f"跳過 {zip_path.name}，缺少欄位：{missing_cols}")
            continue

        df["station_id"] = df["station_id"].astype(str).str.strip()

        # 只取嘉義測站 + 淹水深度
        df = df[
            (df["station_id"].isin(station_ids)) &
            (df["PQ_name"] == "淹水深度")
        ].copy()

        if df.empty:
            continue

        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # 從檔名抓日期，例如 20200101
        match = re.search(r"\d{8}", zip_path.name)
        df["date"] = pd.to_datetime(match.group(), format="%Y%m%d") if match else pd.NaT

        df["source_file"] = zip_path.name

        all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


# =========================
# 主程式
# =========================

chiayi_stations = read_chiayi_stations(STATION_FILE)

chiayi_station_ids = chiayi_stations["station_id"].unique()

chiayi_depth_df = read_depth_data(
    DEPTH_FOLDER,
    chiayi_station_ids
)

# 把站點名稱、經緯度合併回水深資料
station_info = chiayi_stations[[
    "station_id",
    "station_name",
    "area_code",
    "Address",
    "Longitude",
    "Latitude"
]]

chiayi_depth_df = chiayi_depth_df.merge(
    station_info,
    on="station_id",
    how="left"
)

print("嘉義測站數量:", len(chiayi_station_ids))
print("嘉義水深資料筆數:", chiayi_depth_df.shape)
print(chiayi_depth_df.head())
#%% 讀取嘉義("201901", "202512")資料
from pathlib import Path
import pandas as pd
import re


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

STATION_FILE = DATA_DIR / "station_水利署_淹水感測器.csv"
RAW_DEPTH_ROOT = DATA_DIR / "歷年淹水感測資料"
OUTPUT_DIR = DATA_DIR / "processed" / "chiayi_water_depth"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def read_chiayi_stations(station_file):
    """讀取測站清單，篩出嘉義縣市的淹水深度測站"""

    df = pd.read_csv(
        station_file,
        encoding="utf-8-sig",
        low_memory=False
    )

    df.columns = df.columns.str.strip()

    df = df[df["PQ_name"] == "淹水深度"].copy()

    df["area_code"] = (
        df["station_name"]
        .astype(str)
        .str.extract(r"^\d{3}([A-Z])")
    )

    df = df[df["area_code"].isin(["Q", "I"])].copy()

    df = df.drop_duplicates(subset=["station_id"]).copy()

    df["station_id"] = df["station_id"].astype(str).str.strip()

    keep_cols = [
        "station_id",
        "station_name",
        "area_code",
        "OrgName",
        "Code",
        "Address",
        "Longitude",
        "Latitude",
        "PQ_id",
        "PQ_name",
        "SIUnit"
    ]

    return df[keep_cols]


def month_range(start_yyyymm, end_yyyymm):
    """產生 YYYYMM 月份字串，例如 201901 到 202512"""

    months = pd.period_range(
        start=start_yyyymm,
        end=end_yyyymm,
        freq="M"
    )

    return [m.strftime("%Y%m") for m in months]


def read_one_month_depth(depth_folder, station_ids):
    """讀取單一月份資料夾，只保留指定測站與淹水深度"""

    all_dfs = []

    if not depth_folder.exists():
        print("資料夾不存在，跳過:", depth_folder)
        return pd.DataFrame()

    zip_files = sorted(depth_folder.glob("*.zip"))

    if not zip_files:
        print("沒有 zip 檔，跳過:", depth_folder)
        return pd.DataFrame()

    for zip_path in zip_files:
        print("reading:", zip_path.name)

        df = pd.read_csv(
            zip_path,
            compression="zip",
            encoding="utf-8-sig",
            low_memory=False
        )

        df.columns = df.columns.str.strip()

        required_cols = {"station_id", "PQ_name", "value", "timestamp"}
        missing_cols = required_cols - set(df.columns)

        if missing_cols:
            print(f"跳過 {zip_path.name}，缺少欄位：{missing_cols}")
            continue

        df["station_id"] = df["station_id"].astype(str).str.strip()

        df = df[
            (df["station_id"].isin(station_ids)) &
            (df["PQ_name"] == "淹水深度")
        ].copy()

        if df.empty:
            continue

        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        match = re.search(r"\d{8}", zip_path.name)
        df["date"] = (
            pd.to_datetime(match.group(), format="%Y%m%d")
            if match
            else pd.NaT
        )

        df["source_file"] = zip_path.name

        all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def process_months(start_yyyymm, end_yyyymm):
    """逐月處理嘉義淹水深度資料，並輸出每月 CSV"""

    chiayi_stations = read_chiayi_stations(STATION_FILE)
    station_ids = chiayi_stations["station_id"].unique()

    station_info = chiayi_stations[[
        "station_id",
        "station_name",
        "area_code",
        "Address",
        "Longitude",
        "Latitude"
    ]].copy()

    print("嘉義淹水深度測站數:", len(station_ids))

    for yyyymm in month_range(start_yyyymm, end_yyyymm):
        print("=" * 60)
        print("processing month:", yyyymm)

        depth_folder = RAW_DEPTH_ROOT / yyyymm
        output_file = OUTPUT_DIR / f"嘉義_淹水深度_{yyyymm}.csv"

        # 已經處理過就跳過，避免重跑浪費時間
        if output_file.exists():
            print("已存在，跳過:", output_file.name)
            continue

        month_df = read_one_month_depth(
            depth_folder=depth_folder,
            station_ids=station_ids
        )

        if month_df.empty:
            print("本月沒有資料:", yyyymm)
            continue

        month_df = month_df.merge(
            station_info,
            on="station_id",
            how="left"
        )

        # 建議新增乾淨水深欄位：負值視為 0
        month_df["water_depth"] = month_df["value"].clip(lower=0)

        month_df.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig"
        )

        print("已輸出:", output_file)
        print("shape:", month_df.shape)


process_months("201901", "202512")
#%%
from pathlib import Path
import pandas as pd
import re

folder = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "歷年淹水感測資料"
    / "202001"
)

all_dfs = []

for zip_path in sorted(folder.glob("*.zip")):
    print("reading:", zip_path.name)

    df = pd.read_csv(
        zip_path,
        compression="zip",
        encoding="utf-8-sig",
        low_memory=False
    )

    # 從檔名抓 8 位數日期，例如 20181103
    match = re.search(r"\d{8}", zip_path.name)

    if match:
        date_str = match.group()  # '20181103'
        df["date"] = pd.to_datetime(date_str, format="%Y%m%d")
    else:
        df["date"] = pd.NaT

    # 也可以保留來源檔名，方便追蹤
    df["source_file"] = zip_path.name

    all_dfs.append(df)

# 合併所有日期的資料
merged_df = pd.concat(all_dfs, ignore_index=True)

print("merged shape:", merged_df.shape)
print(merged_df.head())