import pandas as pd
import oracledb


csv_path = r'C:\Users\Kaleb\OneDrive\Desktop\fintech-review-analysis\fintech-review-analysis\processed_data\reviews_with_themes.csv'
df = pd.read_csv(csv_path)

# === Step 2: Connect to Oracle XE (Thin mode) ===
connection = oracledb.connect(
    user="bank_reviews",
    password="kaleb21",   # 🔁 Replace this
    dsn="localhost/XEPDB1"
)
cursor = connection.cursor()

# === Step 3: Ensure all banks are in the banks table ===
bank_name_to_id = {}

for bank_name in df['bank_name'].unique():
    cursor.execute("""
        MERGE INTO banks b
        USING (SELECT :bank_name AS bank_name FROM dual) d
        ON (b.bank_name = d.bank_name)
        WHEN NOT MATCHED THEN
            INSERT (bank_name) VALUES (:bank_name)
    """, {'bank_name': bank_name})
    connection.commit()

    cursor.execute("SELECT bank_id FROM banks WHERE bank_name = :bank_name", {'bank_name': bank_name})
    bank_id = cursor.fetchone()[0]
    bank_name_to_id[bank_name] = bank_id

# === Step 4: Insert data into the reviews table ===
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO reviews (
            review_text,
            rating,
            review_date,
            source,
            vader_compound,
            textblob_polarity,
            ensemble_sentiment,
            theme,
            bank_id
        ) VALUES (
            :review_text,
            :rating,
            TO_DATE(:review_date, 'DD/MM/YYYY'),
            :source,
            :vader_compound,
            :textblob_polarity,
            :ensemble_sentiment,
            :theme,
            :bank_id
        )
    """, {
        'review_text': row['review_text'],
        'rating': row['rating'],
        'review_date': row['date'],
        'source': row['source'],
        'vader_compound': row['vader_compound'],
        'textblob_polarity': row['textblob_polarity'],
        'ensemble_sentiment': row['ensemble_sentiment'],
        'theme': row['themes'],
        'bank_id': bank_name_to_id[row['bank_name']]
    })

# === Step 5: Clean up ===
connection.commit()
cursor.close()
connection.close()

print("✅ Data successfully inserted into Oracle database.")


