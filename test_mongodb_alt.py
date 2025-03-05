#!/usr/bin/env python3
import os
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Replace these with your actual credentials
    username = "LOS"  # Use your actual username
    password = "cJKT48lRpn1uZrMA"  # Use your actual password
    host = "cluster0.4qtz1.mongodb.net"

    # Create connection string
    mongo_uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority&authSource=admin"
    logger.info(f"Trying connection with MongoDB Atlas...")

    # Create client
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

    # Test connection
    client.admin.command('ping')
    logger.info("✅ Connection successful!")

    # List databases
    dbs = client.list_database_names()
    logger.info(f"Available databases: {dbs}")

except Exception as e:
    logger.error(f"❌ Connection failed: {str(e)}")