import os
import pandas as pd

# Create output directory
os.makedirs('clean_data', exist_ok=True)

# Load and merge all CSVs
data_dir = 'data'
all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]

df_list = []
for file in all_files:
    print(f"🔍 Loading: {file}")
    df = pd.read_csv(file)
    df_list.append(df)

all_reviews = pd.concat(df_list, ignore_index=True)

# Drop rows with missing values in important columns
all_reviews.dropna(subset=['review_text', 'rating', 'date'], inplace=True)

# Drop duplicates
all_reviews.drop_duplicates(subset=['review_text', 'date', 'bank_name'], inplace=True)

# Normalize date format to YYYY-MM-DD
all_reviews['date'] = pd.to_datetime(all_reviews['date']).dt.strftime('%Y-%m-%d')

# Save cleaned CSV
output_file = 'clean_data/clean_reviews.csv'
all_reviews.to_csv(output_file, index=False)
print(f"✅ Cleaned data saved to {output_file}")
