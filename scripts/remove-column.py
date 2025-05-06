import pandas as pd

print("Loading csv")
df = pd.read_csv('blockgroup_poi.csv')
print("Done loading csv, dropping poi_list_center")
df = df.drop('poi_list_center', axis=1)
print("Done dropping column, saving to disk")
df.to_csv('blockgroup_poi-no-list.csv', index=False)
