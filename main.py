import time, requests
import pandas as pd
from google.cloud import bigquery

PROJECT = "dbt-bigquery-project-507410"
DATASET = "dbt_ecommerce"
RAW_TBL = f"{PROJECT}.{DATASET}.raw_live_weather_forecast"
FCT_TBL = f"{PROJECT}.{DATASET}.fct_station_daily_forecast"
client = bigquery.Client(project=PROJECT)

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
    data = requests.get(url, timeout=30).json()
    for idx, loc in enumerate(data if isinstance(data, list) else [data]):
        d = loc.get("daily", {})
        for k, f_date in enumerate(d.get("time", [])):
            rows.append({"station_id": str(b.iloc[idx]["station_id"]), "forecast_date": f_date, "live_high_temp_c": d["temperature_2m_max"][k], "live_low_temp_c": d["temperature_2m_min"][k], "live_avg_temp_c": d["temperature_2m_mean"][k], "live_precip_mm": d["precipitation_sum"][k], "live_rain_prob_pct": d["precipitation_probability_max"][k], "live_wind_kmh": d["windspeed_10m_max"][k], "weather_code": d["weathercode"][k]})
    time.sleep(2.5)

df = pd.DataFrame(rows)
df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
df["fetched_at"] = pd.Timestamp.utcnow()
client.load_table_from_dataframe(df, RAW_TBL, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
print(f"Loaded {len(df)} rows.")
client.query(f"""
MERGE `{FCT_TBL}` T
USING (
  SELECT trim(station_id) AS station_id, cast(forecast_date AS DATE) AS forecast_date,
    round(live_avg_temp_c, 1) AS avg_t, round(live_high_temp_c, 1) AS hi_t, round(live_low_temp_c, 1) AS lo_t,
    round(live_precip_mm, 1) AS precip, round(live_wind_kmh, 1) AS wind, round(live_rain_prob_pct, 0) AS rain_p,
    CASE WHEN weather_code IN (71,73,75,77,85,86) THEN 80 ELSE 0 END AS snow_p
  FROM `{RAW_TBL}`
) S ON trim(T.station_id) = S.station_id AND T.forecast_date = S.forecast_date
WHEN MATCHED THEN UPDATE SET
  T.forecast_source = 'Live (Open-Meteo)', T.forecast_avg_temp_c = S.avg_t, T.forecast_high_temp_c = S.hi_t,
  T.forecast_low_temp_c = S.lo_t, T.forecast_precip_mm = S.precip, T.forecast_wind_kmh = S.wind,
  T.rain_probability_pct = S.rain_p, T.snow_probability_pct = S.snow_p,
  T.forecast_condition = CASE WHEN S.snow_p >= 30 THEN 'Snow Likely' WHEN S.rain_p >= 40 THEN 'Rainy' WHEN S.avg_t >= 28 THEN 'Hot & Clear' WHEN S.avg_t >= 18 THEN 'Warm & Mild' WHEN S.avg_t >= 8 THEN 'Cool' ELSE 'Cold' END
""").result()
print("Merged into fct_station_daily_forecast!")
