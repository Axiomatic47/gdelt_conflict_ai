#!/usr/bin/env python3
"""
Test MongoDB Connection Script

This script tests the connection to MongoDB Atlas with the updated secure connection parameters.
Run this script to verify your MongoDB connection is working properly.
"""
import os
import sys
import certifi
import ssl
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_connection():
    """Test MongoDB connection with proper SSL/TLS settings"""
    # Load environment variables
    load_dotenv()

    # Get MongoDB URI from environment
    mongo_uri = os.getenv("MONGODB_URI")

    if not mongo_uri:
        logger.error("❌ MONGODB_URI environment variable not found!")
        logger.info("Please ensure you have a .env file with MONGODB_URI defined")
        return False

    logger.info(f"MongoDB URI found: {mongo_uri.split('@')[0]}@...")

    try:
        # Connect to MongoDB with proper SSL configuration
        logger.info("Attempting to connect to MongoDB Atlas...")
        client = MongoClient(
            mongo_uri,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=10000
        )

        # Force a connection test
        client.admin.command('ping')

        # If we get here, connection was successful
        logger.info("✅ Successfully connected to MongoDB Atlas!")

        # Get available databases
        databases = client.list_database_names()
        logger.info(f"Available databases: {databases}")

        # Check for gdelt_db
        if "gdelt_db" in databases:
            db = client["gdelt_db"]
            collections = db.list_collection_names()
            logger.info(f"Collections in gdelt_db: {collections}")

            # Count documents in collections
            for collection in collections:
                count = db[collection].count_documents({})
                logger.info(f"  - {collection}: {count} documents")

        return True

    except Exception as e:
        logger.error(f"❌ Connection to MongoDB Atlas failed: {str(e)}")

        # Provide troubleshooting tips based on error
        if "SSL" in str(e) or "TLS" in str(e):
            logger.info("\nTroubleshooting SSL/TLS issues:")
            logger.info("1. Make sure you have the latest certifi package: pip install --upgrade certifi")
            logger.info("2. Make sure your MongoDB Atlas cluster has TLS enabled")
            logger.info("3. Check if your IP address is whitelisted in MongoDB Atlas Network Access settings")

        elif "authentication failed" in str(e).lower():
            logger.info("\nTroubleshooting authentication issues:")
            logger.info("1. Verify your username and password in the MongoDB URI")
            logger.info("2. Check if the user has appropriate permissions")

        elif "timed out" in str(e).lower():
            logger.info("\nTroubleshooting timeout issues:")
            logger.info("1. Check your network connection")
            logger.info("2. Verify your Atlas cluster is running and not in suspended state")
            logger.info("3. Make sure your IP address is whitelisted in MongoDB Atlas Network Access settings")

        return False


if __name__ == "__main__":
    success = test_connection()
    if success:
        sys.exit(0)
    else:
        sys.exit(1)