import time
import schedule
from store_bigquery_to_mongo import fetch_and_store_gdelt
from nlp_analysis import run_nlp_processing
from store_nlp_results import store_nlp_results
from visualize_nlp_results import generate_nlp_charts
from visualize_conflict_map import generate_conflict_map

def run_pipeline():
    print("\n🚀 Starting Automated Data Collection Pipeline...\n")

    try:
        print("\n📡 Step 1: Fetching new GDELT conflict data...")
        fetch_and_store_gdelt()

        print("\n🧠 Step 2: Running NLP analysis on new data...")
        run_nlp_processing()

        print("\n💾 Step 3: Storing NLP results into MongoDB...")
        store_nlp_results()

        print("\n📊 Step 4: Updating visualizations...")
        generate_nlp_charts()
        generate_conflict_map()

        print("\n✅ Pipeline execution completed!\n")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")

# Schedule the pipeline to run every 6 hours
schedule.every(6).hours.do(run_pipeline)

print("\n⏳ Automation running... Press Ctrl+C to stop.\n")

# Keep the script running indefinitely
while True:
    schedule.run_pending()
    time.sleep(60)
