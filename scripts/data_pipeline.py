import sys
import os
import time
import schedule

# Ensure the `scripts` directory is included in the import path
sys.path.append(os.path.abspath("scripts"))

from bigquery_client import fetch_and_store_gdelt
from nlp_pipeline import run_nlp_processing
from store_gdelt_news import store_nlp_results
from visualize_nlp_results import generate_nlp_charts
from visualize_conflict_map import generate_conflict_map

def run_pipeline():
    """
    Executes the full pipeline in sequence.
    """
    print("\n🚀 Starting Manual Data Collection Pipeline...\n")

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

        print("\n✅ Pipeline execution completed successfully!\n")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")

# Ensure script runs only when executed directly
if __name__ == "__main__":
    run_pipeline()