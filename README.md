# 期末報告

本專案用來整理嘉義地區淹水感測器資料、彙整歷史時雨量資料，並建立雨量與淹水分級之間的基礎機器學習測試流程。

目前流程大致分成三段：

1. 整理嘉義淹水感測站的歷年原始回傳資料
2. 整理指定雨量站的歷史時雨量資料
3. 合併雨量與淹水資料，建立模型測試資料表

## 專案結構

```text
期末報告/
├─ data/
│  ├─ 歷年淹水感測資料/
│  ├─ rainfall/
│  ├─ processed/
│  └─ station_水利署_淹水感測器.csv
├─ output/
│  ├─ C0M650_歷史時雨量.csv
│  ├─ 淹水深度.csv
│  └─ station_location_map.html
├─ script/
│  ├─ 讀取嘉義淹水深度.py
│  ├─ 整理input資料.py
│  ├─ 抓取歷史雨量.py
│  ├─ 測試模型.py
│  └─ 讀單一壓縮檔.py
└─ README.md
```

## 資料說明

### `data/station_水利署_淹水感測器.csv`

水利署淹水感測器測站總表，包含：

- `station_id`
- `station_name`
- `Address`
- `Longitude`
- `Latitude`
- `PQ_name`

目前腳本會依這份總表篩選嘉義地區且 `PQ_name = 淹水深度` 的測站。

### `data/歷年淹水感測資料/`

原始每日淹水感測資料，依月份分資料夾儲存，每天一個 zip 檔。  
zip 內部是 CSV，可直接讀取，不需要先全部解壓縮。

### `data/rainfall/`

歷年時雨量原始資料，命名格式為 `C0M650_YYYY.csv`。

## 主要腳本

### `script/讀取嘉義淹水深度.py`

用途：

- 讀取 `201903` 到 `202512` 的嘉義淹水感測資料
- 整理每月資料
- 分測站輸出歷年資料
- 建立測站摘要

主要輸出位置：

- `data/processed/water_depth_monthly/`
- `data/processed/water_depth_by_station/`
- `data/processed/summaries/station_depth_summary.csv`

補充：

- 會保留 `timestamp_raw`
- 會產生標準化後的 `timestamp`
- 會額外保留 `timestamp_dt` 供排序與分析使用

### `script/整理input資料.py`

用途：

- 讀取嘉義測站歷年淹水資料
- 整理成每小時資料
- 同一時間若有多筆回傳，取最大淹水深度
- 補齊沒有觀測值的時間
- 建立 `water table level`
- 計算有淹水時的統計摘要

目前腳本內可直接看到三個主要變數：

- `raw_df`：原始讀取測站資料
- `hourly_df`：每小時淹水深度資料
- `stats_df`：淹水深度統計結果

目前輸出檔為：

- `output/淹水深度.csv`

### `script/抓取歷史雨量.py`

用途：

- 讀取 `data/rainfall/` 下的 `C0M650_*.csv`
- 統一欄位名稱
- 整理成單一歷史時雨量資料表

目前輸出檔為：

- `output/C0M650_歷史時雨量.csv`

### `script/測試模型.py`

用途：

- 讀取時雨量與淹水深度資料
- 依 `timestamp` 合併
- 建立雨量累積、lag 與最大值特徵
- 切分訓練集與測試集
- 使用 `RandomForestRegressor` 做初步模型測試

目前特徵包含：

- `rain_1h`
- `rain_3h`
- `rain_6h`
- `rain_12h`
- `rain_24h`
- `rain_48h`
- `rain_lag_1h`
- `rain_lag_2h`
- `rain_lag_3h`
- `rain_lag_6h`
- `rain_max_3h`
- `rain_max_6h`
- `rain_max_12h`

目標欄位：

- `water table level`

### `script/讀單一壓縮檔.py`

用途：

- 測試單一 zip 檔是否能直接讀成 DataFrame
- 方便快速檢查原始資料欄位與內容

## 目前主要輸出

### `output/C0M650_歷史時雨量.csv`

整理後的時雨量資料，主要欄位包含：

- `timestamp`
- `rain_station_id`
- `rain_1h`
- `year`
- `source_file`

### `output/淹水深度.csv`

整理後的每小時淹水深度資料，主要欄位包含：

- `timestamp`
- `hourly_water_depth_cm`
- `water table level`

### `data/processed/water_depth_by_station/station_74249b35-891e-479f-ad77-34c6e7434789_201903_202512.csv`

目前嘉義測站整理後的主要歷年淹水資料。

## `water table level` 分級規則

- `淹水深度 = 0` -> `0`
- `0 < 淹水深度 < 1` -> `1`
- `1 <= 淹水深度 < 10` -> `2`
- `10 <= 淹水深度 < 50` -> `3`
- `淹水深度 >= 50` -> `4`

## 建議執行順序

1. 執行 `script/讀取嘉義淹水深度.py`
2. 執行 `script/整理input資料.py`
3. 執行 `script/抓取歷史雨量.py`
4. 執行 `script/測試模型.py`

## 環境建議

目前專案主要使用：

- Python
- pandas
- numpy
- scikit-learn
- matplotlib
- geopandas / rasterio / 其他 GIS 套件（視需求）

建議使用 conda 環境執行。

## 已知問題

### 1. `matplotlib` 在 `gis` 環境可能造成 kernel restart

目前在 `gis` 環境中，`matplotlib` 的 `tight_layout()` 與部分繪圖流程可能觸發 Windows fatal exception。  
若模型訓練已完成但畫圖時 kernel 重啟，通常不是模型失敗，而是繪圖環境問題。

建議：

- 先移除 `tight_layout()`
- 先以表格或 CSV 檢查結果
- 必要時在新的乾淨 conda 環境處理繪圖

### 2. 資料分布高度不平衡

`water table level` 目前大多數時間為 `0`，因此即使模型可執行，初步 `RandomForestRegressor` 的效果可能不理想。  
後續若要提升模型表現，建議考慮：

- 改成分類模型
- 重新處理類別不平衡
- 增加事件型特徵

## 備註

目前部分腳本內容曾出現中文編碼亂碼的情況。  
若後續要整理或交付，建議優先統一：

- 檔案編碼為 UTF-8
- 輸出欄位名稱
- 腳本中的中文註解與路徑字串
