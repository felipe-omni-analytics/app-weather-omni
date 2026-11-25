import time, requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery

PROJECT = "dbt-bigquery-project-507410"
DATASET = "dbt_ecommerce"
RESORT_TBL = f"{PROJECT}.{DATASET}.dim_ski_resorts"
STG_TBL = f"{PROJECT}.{DATASET}.stg_resort_snow_forecast"
FCT_TBL = f"{PROJECT}.{DATASET}.fct_resort_snow_forecast"

client = bigquery.Client(project=PROJECT)
session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

resorts = client.query(f"SELECT * FROM `{RESORT_TBL}`").to_dataframe()

points = []
for _, r in resorts.iterrows():
    for band, col in [("Base", "base_elev_m"), ("Mid", "mid_elev_m"), ("Summit", "summit_elev_m")]:
        points.append({
            "resort_id": r["resort_id"],
            "resort_name": r["resort_name"],
            "country": r["country"],
            "region": r["region"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "elevation_band": band,
            "elevation_m": int(r[col])
        })
pts_df = pd.DataFrame(points)

rows = []
for i in range(0, len(pts_df), 50):
    b = pts_df.iloc[i:i+50]
    lats = ",".join(b["latitude"].astype(str))
    lons = ",".join(b["longitude"].astype(str))
    elevs = ",".join(b["elevation_m"].astype(str))
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}&elevation={elevs}"
        f"&daily=snowfall_sum,snow_depth_max,freezing_level_height_mean,temperature_2m_max,"
        f"temperature_2m_min,wind_speed_10m_max,wind_gusts_10m_max,weather_code"
        f"&forecast_days=16&timezone=UTC"
    )
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    for idx, loc in enumerate(data if isinstance(data, list) else [data]):
        d = loc.get("daily", {})
        meta = b.iloc[idx]
        for k, f_date in enumerate(d.get("time", [])):
            snow_cm = d["snowfall_sum"][k] or 0.0
            depth_m = d["snow_depth_max"][k] or 0.0
            fl_m = d["freezing_level_height_mean"][k]
            rows.append({
                "resort_id": meta["resort_id"],
                "resort_name": meta["resort_name"],
                "country": meta["country"],
                "region": meta["region"],
                "latitude": float(meta["latitude"]),
                "longitude": float(meta["longitude"]),
                "elevation_band": meta["elevation_band"],
                "elevation_m": int(meta["elevation_m"]),
                "forecast_date": f_date,
                "snowfall_cm": round(float(snow_cm), 1),
                "snow_depth_cm": round(float(depth_m) * 100, 1),
                "freezing_level_m": int(round(fl_m)) if fl_m is not None else None,
                "high_temp_c": d["temperature_2m_max"][k],
                "low_temp_c": d["temperature_2m_min"][k],
                "wind_max_kmh": d["wind_speed_10m_max"][k],
                "wind_gusts_kmh": d["wind_gusts_10m_max"][k],
                "weather_code": d["weather_code"][k],
                "powder_alert": "Powder Alert" if snow_cm >= 15 else ("Light Snow" if snow_cm >= 2 else "Dry / Clear")
            })
    print(f"Batch {i//50 + 1} complete")
    time.sleep(1.5)

df = pd.DataFrame(rows)
df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
df["fetched_at"] = pd.Timestamp.now("UTC")

client.load_table_from_dataframe(df, STG_TBL, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
client.query(f"DROP TABLE IF EXISTS `{FCT_TBL}`").result()
client.query(f"CREATE TABLE `{FCT_TBL}` CLUSTER BY resort_id, elevation_band AS SELECT * FROM `{STG_TBL}`").result()
client.query(f"DROP TABLE IF EXISTS `{STG_TBL}`").result()
print(f"Loaded {len(df)} resort-elevation forecast rows into {FCT_TBL}!")
