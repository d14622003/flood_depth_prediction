# -*- coding: utf-8 -*-
"""
Created on Mon May  4 21:42:20 2026

@author: Nitro
"""

#%% 1. 讀取兩個檔案，並以 timestamp 作為 index

from pathlib import Path
import pandas as pd

rain_file = Path(r"D:\Nitro\桌面\遙測與空間資訊之分析與應用\期末報告\output\C0M650_歷史時雨量.csv")
depth_file = Path(r"D:\Nitro\桌面\遙測與空間資訊之分析與應用\期末報告\output\淹水深度.csv")

# 讀取資料
rain = pd.read_csv(
    rain_file,
    encoding="utf-8-sig",
    low_memory=False
)

depth = pd.read_csv(
    depth_file,
    encoding="utf-8-sig",
    low_memory=False
)

# 清理欄位名稱
rain.columns = rain.columns.str.strip()
depth.columns = depth.columns.str.strip()

# 檢查欄位
print("rain columns:")
print(rain.columns.tolist())

print("\ndepth columns:")
print(depth.columns.tolist())
#%% 2. 轉換 timestamp 欄位

rain["timestamp"] = pd.to_datetime(
    rain["timestamp"],
    errors="coerce"
)

depth["timestamp"] = pd.to_datetime(
    depth["timestamp"],
    errors="coerce"
)

# 移除 timestamp 解析失敗的資料
rain = rain.dropna(subset=["timestamp"]).copy()
depth = depth.dropna(subset=["timestamp"]).copy()

# 設定 timestamp 為 index
rain = rain.set_index("timestamp").sort_index()
depth = depth.set_index("timestamp").sort_index()

print("rain index:")
print(rain.index.min(), "~", rain.index.max())

print("\ndepth index:")
print(depth.index.min(), "~", depth.index.max())
#%% 3. 確保主要數值欄位為 numeric

# 雨量欄位
rain["rain_1h"] = pd.to_numeric(
    rain["rain_1h"],
    errors="coerce"
)

# 負雨量通常代表缺值或異常值
rain.loc[rain["rain_1h"] < 0, "rain_1h"] = pd.NA

# 淹水深度欄位
depth["hourly_water_depth_cm"] = pd.to_numeric(
    depth["hourly_water_depth_cm"],
    errors="coerce"
)

# 負水深先視為 0
depth["hourly_water_depth_cm"] = depth["hourly_water_depth_cm"].clip(lower=0)
#%% 4. 合併兩份資料

# 取出需要欄位
rain_use = rain[["rain_1h"]].copy()

water_depth_cm = depth[["hourly_water_depth_cm"]].copy()

water_depth_level = depth[["water table level"]].copy()

# 依 timestamp index 合併
merged_df = rain_use.join(
    water_depth_cm ,
    how="inner"
).join(
    water_depth_level,
    how="inner"
)

print(merged_df.head())
print(merged_df.shape)
#%% 模型建立
#%% 5. 建立機器學習用資料表

import pandas as pd
import numpy as np

df = merged_df.copy()
df = df.sort_index()

# 確保欄位為數值
df["rain_1h"] = pd.to_numeric(df["rain_1h"], errors="coerce")
df["water table level"] = pd.to_numeric(df["water table level"], errors="coerce")
df["hourly_water_depth_cm"] = pd.to_numeric(df["hourly_water_depth_cm"], errors="coerce")

# 負雨量視為 0
df["rain_1h"] = df["rain_1h"].clip(lower=0)

# 移除目標值缺失的資料
df = df.dropna(subset=["rain_1h", "water table level"]).copy()

print(df.head())
print(df.describe())
#%% 6. 建立雨量特徵

df["rain_3h"] = df["rain_1h"].rolling(window=3, min_periods=1).sum()
df["rain_6h"] = df["rain_1h"].rolling(window=6, min_periods=1).sum()
df["rain_12h"] = df["rain_1h"].rolling(window=12, min_periods=1).sum()
df["rain_24h"] = df["rain_1h"].rolling(window=24, min_periods=1).sum()
df["rain_48h"] = df["rain_1h"].rolling(window=48, min_periods=1).sum()

# 前幾小時雨量
df["rain_lag_1h"] = df["rain_1h"].shift(1)
df["rain_lag_2h"] = df["rain_1h"].shift(2)
df["rain_lag_3h"] = df["rain_1h"].shift(3)
df["rain_lag_6h"] = df["rain_1h"].shift(6)

# 過去幾小時最大時雨量
df["rain_max_3h"] = df["rain_1h"].rolling(window=3, min_periods=1).max()
df["rain_max_6h"] = df["rain_1h"].rolling(window=6, min_periods=1).max()
df["rain_max_12h"] = df["rain_1h"].rolling(window=12, min_periods=1).max()

print(df[[
    "rain_1h",
    "rain_3h",
    "rain_6h",
    "rain_12h",
    "rain_24h",
    "water table level"
]].head())
#%% 7. 設定特徵與目標

features = [
    "rain_1h",
    "rain_3h",
    "rain_6h",
    "rain_12h",
    "rain_24h",
    "rain_48h",
    "rain_lag_1h",
    "rain_lag_2h",
    "rain_lag_3h",
    "rain_lag_6h",
    "rain_max_3h",
    "rain_max_6h",
    "rain_max_12h"
]

target = "water table level"

model_data = df.dropna(subset=features + [target]).copy()

X = model_data[features]
y = model_data[target]

print("model_data shape:", model_data.shape)
print(X.head())
print(y.head())
#%% 8. 時間序列切分資料

split_index = int(len(model_data) * 0.8)

train = model_data.iloc[:split_index].copy()
test = model_data.iloc[split_index:].copy()

X_train = train[features]
y_train = train[target]

X_test = test[features]
y_test = test[target]

print("train:", train.shape)
print("test:", test.shape)

print("train time:", train.index.min(), "~", train.index.max())
print("test time:", test.index.min(), "~", test.index.max())

#%% 9. Random Forest Regressor

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

model = RandomForestRegressor(
    n_estimators=300,
    max_depth=8,
    min_samples_leaf=5,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

y_pred = model.predict(X_test)

test["pred_water_level"] = y_pred
#%% 10. 評估模型表現

mae = mean_absolute_error(y_test, y_pred)
rmse = mean_squared_error(y_test, y_pred) ** 0.5
r2 = r2_score(y_test, y_pred)

print("MAE:", mae)
print("RMSE:", rmse)
print("R²:", r2)
#%% 11. 畫圖比較

import matplotlib.pyplot as plt

plt.figure(figsize=(12, 4))
plt.plot(test.index, test["water table level"], label="Observed")
plt.plot(test.index, test["pred_water_level"], label="Predicted")
plt.xlabel("Time")
plt.ylabel("Water table level")
plt.title("Random Forest: Observed vs Predicted Water Table Level")
plt.legend()

plt.show()
#%% 繪製資料原始圖
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

rain_file = Path(r"D:\Nitro\桌面\遙測與空間資訊之分析與應用\期末報告\output\C0M650_歷史時雨量.csv")
depth_file = Path(r"D:\Nitro\桌面\遙測與空間資訊之分析與應用\期末報告\output\淹水深度.csv")

# 讀取資料
rain = pd.read_csv(rain_file, encoding="utf-8-sig", low_memory=False)
depth = pd.read_csv(depth_file, encoding="utf-8-sig", low_memory=False)

# 清理欄位名稱
rain.columns = rain.columns.str.strip()
depth.columns = depth.columns.str.strip()

# 時間轉換
rain["timestamp"] = pd.to_datetime(rain["timestamp"], errors="coerce")
depth["timestamp"] = pd.to_datetime(depth["timestamp"], errors="coerce")

# 數值轉換
rain["rain_1h"] = pd.to_numeric(rain["rain_1h"], errors="coerce")
depth["hourly_water_depth_cm"] = pd.to_numeric(
    depth["hourly_water_depth_cm"],
    errors="coerce"
)

# 移除時間錯誤資料
rain = rain.dropna(subset=["timestamp"]).copy()
depth = depth.dropna(subset=["timestamp"]).copy()

# 設定 timestamp 為 index
rain = rain.set_index("timestamp").sort_index()
depth = depth.set_index("timestamp").sort_index()

# 可選：只畫某一段時間，避免整段太擠
start = "2019-03-01"
end = "2025-12-31"

rain_plot = rain.loc[start:end].copy()
depth_plot = depth.loc[start:end].copy()

# 上下圖，共用同一個 x 軸
fig, axes = plt.subplots(
    nrows=2,
    ncols=1,
    figsize=(12, 6),
    sharex=True
)

# 上圖：雨量
axes[0].bar(
    rain_plot.index,
    rain_plot["rain_1h"],
    width=0.03
)
axes[0].set_ylabel("Rainfall (mm/hr)")
axes[0].set_title("Hourly Rainfall - C0M650")

# 下圖：淹水深度
axes[1].plot(
    depth_plot.index,
    depth_plot["hourly_water_depth_cm"]
)
axes[1].set_ylabel("Flood depth (cm)")
axes[1].set_xlabel("Time")
axes[1].set_title("Hourly Flood Depth")

plt.tight_layout()
plt.show()