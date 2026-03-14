from transform import df
df.to_parquet("data/WHO-COVID-19-global-daily-data.parquet", engine="pyarrow")