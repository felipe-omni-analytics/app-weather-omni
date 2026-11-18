import time, requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery

PROJECT = "dbt-bigquery-project-507410"
DATASET = "dbt_ecommerce"
RAW_TBL = f"{PROJECT}.{DATASET}.raw_live_weather_forecast"
FCT_TBL = f"{PROJECT}.{DATASET}.fct_station_daily_forecast"
client = bigquery.Client(project=PROJECT)

session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

stations = client.query(f"""
  SELECT station_id, latitude, longitude
  FROM `{PROJECT}.{DATASET}.dim_global_weather_stations`
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
  ORDER BY CASE WHEN country_code IN ('EI','UK','US','BR','FR','GM','ES','IT','JP','AU') THEN 0 ELSE 1 END, FARM_FINGERPRINT(station_id)
  LIMIT 500
""").to_dataframe()

rows = []
for i in range(0, len(stations), 50):
    b = stations.iloc[i:i+50]
    lats, lons = ",".join(b["latitude"].astype(str)), ",".join(b["longitude"].astype(str))
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,precipitation_probability_max,windspeed_10m_max,weathercode&forecast_days=16&timezone=UTC"
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    for idx, loc in enumerate(data if isinstance(data, list) else [data]):
        d = loc.get("daily", {})
        for k, f_date in enumerate(d.get("time", [])):
            rows.append({"station_id": str(b.iloc[idx]["station_id"]), "forecast_date": f_date, "live_high_temp_c": d["temperature_2m_max"][k], "live_low_temp_c": d["temperature_2m_min"][k], "live_avg_temp_c": d["temperature_2m_mean"][k], "live_precip_mm": d["precipitation_sum"][k], "live_rain_prob_pct": d["precipitation_probability_max"][k], "live_wind_kmh": d["windspeed_10m_max"][k], "weather_code": d["weathercode"][k]})
    print(f"Batch {i//50 + 1}/10 complete")
    time.sleep(2.5)

df = pd.DataFrame(rows)
df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
df["fetched_at"] = pd.Timestamp.now("UTC")
client.load_table_from_dataframe(df, RAW_TBL, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
STG_TBL = f"{PROJECT}.{DATASET}.stg_station_daily_forecast"

client.query(f"""
CREATE OR REPLACE TABLE `{STG_TBL}` CLUSTER BY station_id, month_num AS
WITH live AS (
  SELECT trim(station_id) AS station_id, cast(forecast_date AS DATE) AS forecast_date,
    round(live_avg_temp_c, 1) AS avg_t, round(live_high_temp_c, 1) AS hi_t, round(live_low_temp_c, 1) AS lo_t,
    round(live_precip_mm, 1) AS precip, round(live_wind_kmh, 1) AS wind, round(live_rain_prob_pct, 0) AS rain_p,
    CASE WHEN weather_code IN (71,73,75,77,85,86) THEN 80 ELSE 0 END AS snow_p
  FROM `{RAW_TBL}`
)
SELECT
  T.* EXCEPT(forecast_source, forecast_avg_temp_c, forecast_high_temp_c, forecast_low_temp_c, forecast_precip_mm, forecast_wind_kmh, rain_probability_pct, snow_probability_pct, forecast_condition),
  IF(S.station_id IS NOT NULL, 'Live (Open-Meteo)', T.forecast_source) AS forecast_source,
  COALESCE(S.avg_t, T.forecast_avg_temp_c) AS forecast_avg_temp_c,
  COALESCE(S.hi_t, T.forecast_high_temp_c) AS forecast_high_temp_c,
  COALESCE(S.lo_t, T.forecast_low_temp_c) AS forecast_low_temp_c,
  COALESCE(S.precip, T.forecast_precip_mm) AS forecast_precip_mm,
  COALESCE(S.wind, T.forecast_wind_kmh) AS forecast_wind_kmh,
  COALESCE(S.rain_p, T.rain_probability_pct) AS rain_probability_pct,
  COALESCE(S.snow_p, T.snow_probability_pct) AS snow_probability_pct,
  CASE
    WHEN S.station_id IS NULL THEN T.forecast_condition
    WHEN S.snow_p >= 30 THEN 'Snow Likely'
    WHEN S.rain_p >= 40 THEN 'Rainy'
    WHEN S.avg_t >= 28 THEN 'Hot & Clear'
    WHEN S.avg_t >= 18 THEN 'Warm & Mild'
    WHEN S.avg_t >= 8 THEN 'Cool'
    ELSE 'Cold'
  END AS forecast_condition
FROM `{FCT_TBL}` T
LEFT JOIN live S ON trim(T.station_id) = S.station_id AND T.forecast_date = S.forecast_date
""").result()

client.query(f"DROP TABLE IF EXISTS `{FCT_TBL}`").result()
client.query(f"CREATE TABLE `{FCT_TBL}` CLUSTER BY station_id, month_num AS SELECT * FROM `{STG_TBL}`").result()
client.query(f"DROP TABLE IF EXISTS `{STG_TBL}`").result()
print("Blended fct_station_daily_forecast successfully!")
