import pandas as pd
from collections import Counter

# Load the JSON file (newline-delimited JSON objects)
df = pd.read_json('yelp.json', lines=True)

# Drop missing values in 'categories' column
df = df.dropna(subset=['categories'])
df = df.dropna(subset=['latitude'])
df = df.dropna(subset=['longitude'])
df = df.dropna(subset=['stars'])

# Split categories, flatten into one list, and strip whitespace
all_categories = df['categories'].str.split(',').explode().str.strip()

# Count occurrences using Counter
category_counts = Counter(all_categories)

# Print categories from most to least occurrences
for category, count in category_counts.most_common():
    print(f"{category}: {count}")

exit()

threshold = 100  # Minimum number of occurrences to be kept separate

condensed_categories = []

for category, count in category_counts.items():
    if count >= threshold:
        condensed_categories.append(category)
print(condensed_categories)
print(len(condensed_categories))
