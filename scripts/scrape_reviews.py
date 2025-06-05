from google_play_scraper import Sort, reviews
import csv
from datetime import datetime
import logging
import os

# ------------------ Setup ------------------ #
# Create logs and data directories
os.makedirs('data', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Set up logging
logging.basicConfig(
    filename='logs/scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ------------------ App List ------------------ #
bank_apps = [
    {
        "app_id": "com.combanketh.mobilebanking",
        "bank_name": "Commercial Bank of Ethiopia"
    },
    {
        "app_id": "com.boa.boaMobileBanking",
        "bank_name": "Bank of Abyssinia"
    },
    {
        "app_id": "com.dashen.dashensuperapp",
        "bank_name": "Dashen Bank"
    }
]

# ------------------ Scraper Function ------------------ #
def scrape_reviews_for_bank(app_id, bank_name, review_count=400):
    logging.info(f"🔄 Scraping reviews for {bank_name}...")

    try:
        result, _ = reviews(
            app_id,
            lang='en',
            country='ET',
            sort=Sort.NEWEST,
            count=review_count,
            filter_score_with=None
        )

        # Output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_name = bank_name.replace(" ", "_")
        filename = f"data/{safe_name}_reviews_{timestamp}.csv"

        # Write to CSV
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['review_text', 'rating', 'date', 'bank_name', 'source'])
            writer.writeheader()

            for entry in result:
                writer.writerow({
                    'review_text': entry['content'],
                    'rating': entry['score'],
                    'date': entry['at'].strftime('%Y-%m-%d'),
                    'bank_name': bank_name,
                    'source': 'Google Play'
                })

        logging.info(f"✅ Saved {len(result)} reviews to {filename}")
        print(f"✅ {bank_name}: {len(result)} reviews saved to {filename}")

    except Exception as e:
        logging.error(f"❌ Error scraping {bank_name}: {e}")
        print(f"❌ Error scraping {bank_name}: {e}")

# ------------------ Main Execution ------------------ #
if __name__ == "__main__":
    for bank in bank_apps:
        scrape_reviews_for_bank(bank["app_id"], bank["bank_name"])
