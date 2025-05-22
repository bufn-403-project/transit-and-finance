import pandas as pd

df_1 = pd.read_csv("/Users/omduggineni/Downloads/final report data/blockgroup_areas.csv", dtype={"GEOID10": str})
df_2 = pd.read_csv("/Users/omduggineni/Downloads/final report data/blockgroup_poi-no-list.csv", dtype={"GEOID10": str})
df_3 = pd.read_csv("/Users/omduggineni/Downloads/final report data/iso_features.csv", dtype={"GEOID10": str})

df_1["GEOID10"] = df_1["GEOID10"].str.replace(".0", "").astype(int)

df_2["GEOID10"] = df_2["GEOID10"].str.replace(".0", "").astype(int)
df_2["profile"] = df_2["profile"].astype("category")

df_3 = df_3.rename(columns={"choice": "is_choice_neighborhood"})
df_3["GEOID10"] = df_3["isochrone"].str.split("-").apply(lambda x: x[0]).astype(int)
df_3["isochrone_type"] = df_3["isochrone"].str.split("-").apply(lambda x: f"{x[1]}-{x[2]}")
df_3["profile"] = df_3["isochrone_type"].str.split("-").apply(lambda x: x[1])
df_3["time_limit"] = df_3["isochrone_type"].str.split("-").apply(lambda x: int(x[0]))
del df_3["isochrone_type"]
del df_3["Unnamed: 0"]
del df_3["isochrone"]
df_3["center_latitude"] = df_3["center"].str.split(",").apply(lambda x: x[0]).astype(float)
df_3["center_longitude"] = df_3["center"].str.split(",").apply(lambda x: x[0]).astype(float)
del df_3["center"]

df_merged = pd.merge(df_1, df_2, on=["GEOID10", "profile", "time_limit"], how="inner")
df_merged = pd.merge(df_merged, df_3, on=["GEOID10", "profile", "time_limit"], how="inner")

assert (set(df_merged["GEOID10"]) - set(df_1["GEOID10"])) == set()
assert (set(df_merged["GEOID10"]) - set(df_2["GEOID10"])) == set()
assert (set(df_merged["GEOID10"]) - set(df_3["GEOID10"])) == set()

print(df_merged.columns)

df_merged.to_parquet("blockgroup-data.parquet", compression="zstd", compression_level=122)