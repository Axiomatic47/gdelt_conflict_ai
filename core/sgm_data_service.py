#!/usr/bin/env python3
"""
Improved SGM Data Service - Fixed to handle GDELT data properly
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any
import random

from google.cloud import bigquery
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Initialize BigQuery Client
try:
    bq_client = bigquery.Client()
    print("✅ Connected to BigQuery successfully!")
except Exception as e:
    print(f"⚠️ Could not connect to BigQuery: {str(e)}")
    bq_client = None

# Connect to MongoDB
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["gdelt_db"]  # Use your existing database
    sgm_collection = db["sgm_scores"]  # Create a new collection for SGM data
    print("✅ Connected to MongoDB successfully!")
except Exception as e:
    print(f"❌ Failed to connect to MongoDB: {str(e)}")
    mongo_client = None


class SGMService:
    """Improved SGM service focusing on GDELT data with proper error handling"""

    def __init__(self):
        self.bq_client = bq_client

    def fetch_conflict_data(self):
        """Fetch conflict-related data from GDELT via BigQuery with improved error handling"""
        print("📊 Fetching conflict data from GDELT...")

        if not self.bq_client:
            print("⚠️ BigQuery client not available, using mock data")
            return self._generate_mock_data()

        # FIXED QUERY: Handle non-numeric EventRootCode values properly
        query = """
        SELECT 
            SQLDATE as date,
            Actor1Name as actor1,
            Actor2Name as actor2,
            CASE 
                WHEN REGEXP_CONTAINS(EventRootCode, r'^[0-9]+$') THEN SAFE_CAST(EventRootCode AS INT64)
                ELSE NULL 
            END as event_code,
            GoldsteinScale as goldstein_scale,
            AvgTone as avg_tone,
            ActionGeo_FullName as location
        FROM `gdelt-bq.gdeltv2.events`
        WHERE REGEXP_CONTAINS(EventRootCode, r'^[0-9]+$') 
          AND SAFE_CAST(EventRootCode AS INT64) BETWEEN 14 AND 19
          AND SQLDATE >= CAST(FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS INT64)
        ORDER BY SQLDATE DESC
        LIMIT 100
        """

        try:
            results = self.bq_client.query(query).result()
            conflict_data = [dict(row) for row in results]
            print(f"✅ Successfully fetched {len(conflict_data)} conflict events from GDELT")
            return conflict_data
        except Exception as e:
            print(f"❌ Error fetching data from BigQuery: {str(e)}")
            # Return mock data for testing if there's an issue
            print("⚠️ Using mock data for testing")
            return self._generate_mock_data()

    def _generate_mock_data(self, num_events=30):
        """Generate mock conflict data for testing"""
        locations = [
            "United States", "China", "Russia", "Germany", "United Kingdom",
            "France", "Japan", "India", "Brazil", "Canada", "Australia"
        ]
        actors = [
            "Government", "Military", "Protesters", "Police", "Workers",
            "Students", "Citizens", "Activists", "Officials", "Rebels"
        ]

        mock_data = []
        for i in range(num_events):
            location = random.choice(locations)
            actor1 = random.choice(actors)
            actor2 = random.choice(actors)
            while actor1 == actor2:
                actor2 = random.choice(actors)

            event = {
                "date": 20240301 - (i % 30),  # Last 30 days
                "actor1": f"{actor1} {location}",
                "actor2": f"{actor2} {location}",
                "event_code": random.randint(14, 19),
                "goldstein_scale": random.uniform(-10, 5),
                "avg_tone": random.uniform(-7, 3),
                "location": location
            }
            mock_data.append(event)

        return mock_data

    def calculate_sgm_scores(self, conflict_data):
        """
        Calculate SGM scores based on available GDELT data
        """
        # Group by country (location)
        country_data = {}
        for event in conflict_data:
            location = event.get('location', 'Unknown')
            country = location.split(',')[-1].strip() if ',' in location else location

            if country not in country_data:
                country_data[country] = {
                    'events': [],
                    'avg_tone_sum': 0,
                    'goldstein_sum': 0
                }

            country_data[country]['events'].append(event)
            country_data[country]['avg_tone_sum'] += event.get('avg_tone', 0)
            country_data[country]['goldstein_sum'] += event.get('goldstein_scale', 0)

        # Calculate scores for each country
        sgm_scores = []
        for country, data in country_data.items():
            event_count = len(data['events'])
            if event_count == 0:
                continue

            # Calculate average tone and Goldstein scale
            avg_tone = data['avg_tone_sum'] / event_count
            avg_goldstein = data['goldstein_sum'] / event_count

            # More negative tone = higher international score (0-10 scale)
            intl_score = min(10, max(0, 5 - (avg_tone / 2)))

            # More negative Goldstein scale = higher domestic score (0-10 scale)
            # Goldstein scale is -10 to 10, so normalize to 0-10
            domestic_score = min(10, max(0, 5 - (avg_goldstein / 2)))

            # Calculate GSCS as average of domestic and international scores
            gscs = (domestic_score + intl_score) / 2

            # Determine category based on GSCS
            category = self._determine_category(gscs)

            # Generate a simple description based on the scores
            description = self._generate_description(country, domestic_score, intl_score, gscs)

            # Calculate a simple stability score (STI)
            # For now, this is randomly distributed between 20-80 based on the GSCS
            # In a real implementation, this would use historical data
            sti = int(gscs * 8) + random.randint(-10, 10)
            sti = max(0, min(100, sti))  # Ensure it's between 0-100

            sgm_scores.append({
                'country': country,
                'code': self._get_country_code(country),
                'srsD': round(domestic_score, 1),
                'srsI': round(intl_score, 1),
                'gscs': round(gscs, 1),
                'sti': sti,
                'category': category,
                'description': description,
                'event_count': event_count,
                'avg_tone': round(avg_tone, 2),
                'avg_goldstein': round(avg_goldstein, 2),
                'updated_at': datetime.now()
            })

        return sgm_scores

    def _determine_category(self, gscs):
        """Determine the supremacism category based on GSCS score"""
        if gscs <= 2:
            return "Non-Supremacist Governance"
        elif gscs <= 4:
            return "Mixed Governance"
        elif gscs <= 6:
            return "Soft Supremacism"
        elif gscs <= 8:
            return "Structural Supremacism"
        else:
            return "Extreme Supremacism"

    def _generate_description(self, country, domestic, international, gscs):
        """Generate a simple description based on the scores"""
        if gscs <= 2:
            return f"{country} demonstrates low levels of supremacism with generally egalitarian governance patterns."
        elif gscs <= 4:
            return f"{country} shows mixed governance with some egalitarian and some supremacist tendencies."
        elif gscs <= 6:
            return f"{country} exhibits soft supremacism with institutional inequalities despite formal legal equality."
        elif gscs <= 8:
            return f"{country} demonstrates structural supremacism with notable inequalities at societal and governmental levels."
        else:
            return f"{country} shows extreme supremacist governance with severe systemic discrimination."

    def _get_country_code(self, country_name):
        """Get country code from name (simplified version)"""
        # This is a very simplified mapping - in production, use a proper country code library
        country_codes = {
            "United States": "US",
            "Russia": "RU",
            "China": "CN",
            "Germany": "DE",
            "France": "FR",
            "United Kingdom": "GB",
            "Japan": "JP",
            "India": "IN",
            "Brazil": "BR",
            "Canada": "CA",
            "Australia": "AU",
            "South Africa": "ZA"
        }

        # Try to match the country name
        for name, code in country_codes.items():
            if name.lower() in country_name.lower():
                return code

        # If no match found, return first two letters as a fallback
        return country_name[:2].upper()

    def store_sgm_scores(self, scores):
        """Store SGM scores in MongoDB - FIXED to use proper bulk operations"""
        if not mongo_client:
            print("⚠️ MongoDB not connected, skipping storage")
            return False

        try:
            # Create proper UpdateOne operations for MongoDB bulk write
            bulk_ops = []
            for score in scores:
                bulk_ops.append(
                    UpdateOne(
                        {"code": score['code']},
                        {"$set": score},
                        upsert=True
                    )
                )

            if bulk_ops:
                sgm_collection.bulk_write(bulk_ops)
                print(f"✅ Successfully stored {len(scores)} SGM scores in MongoDB")
                return True
            else:
                print("⚠️ No scores to store")
                return False

        except Exception as e:
            print(f"❌ Error storing SGM scores in MongoDB: {str(e)}")
            return False

    def run_sgm_analysis(self):
        """Run the complete SGM analysis pipeline"""
        print("🚀 Starting SGM analysis...")

        # Step 1: Fetch conflict data from GDELT
        conflict_data = self.fetch_conflict_data()
        if not conflict_data:
            print("❌ No conflict data available, aborting analysis")
            return False

        # Step 2: Calculate SGM scores
        sgm_scores = self.calculate_sgm_scores(conflict_data)
        if not sgm_scores:
            print("❌ Failed to calculate SGM scores, aborting analysis")
            return False

        # Step 3: Store results in MongoDB
        success = self.store_sgm_scores(sgm_scores)

        print("✅ SGM analysis completed!")
        return success


# Simple CLI for running the service
if __name__ == "__main__":
    print("🌎 SGM Data Service - GDELT Integration")
    service = SGMService()
    service.run_sgm_analysis()