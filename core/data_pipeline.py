import time
import schedule
from core.bigquery_client import fetch_and_store_gdelt
from core.nlp_pipeline import run_nlp_processing
from core.visualize_nlp_results import generate_nlp_charts
from core.visualize_conflict_map import generate_conflict_map

def run_pipeline():
    print("\n🚀 Running Automated Data Collection Pipeline...\n")

    try:
        print("\n📡 Fetching GDELT conflict data...")
        fetch_and_store_gdelt()

        print("\n🧠 Running NLP analysis...")
        run_nlp_processing()

        print("\n📊 Updating visualizations...")
        generate_nlp_charts()
        generate_conflict_map()

        print("\n✅ Pipeline execution completed!\n")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")

schedule.every(6).hours.do(run_pipeline)

print("\n⏳ Automation running... Press Ctrl+C to stop.\n")

while True:
    schedule.run_pending()
    time.sleep(60)