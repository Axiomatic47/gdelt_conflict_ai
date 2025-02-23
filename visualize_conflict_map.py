import folium
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["nlp_analysis_results"]

print("\n🔍 Fetching geospatial conflict data from MongoDB...")

# Fetch data
data = list(collection.find({"location": {"$exists": True}, "sentiment_transformers": {"$exists": True}}))
df = pd.DataFrame(data)

if df.empty:
    print("❌ No conflict data found with geolocation and sentiment analysis. Exiting.")
    exit()

# Initialize geocoder
geolocator = Nominatim(user_agent="geo_locator")

# Function to geocode locations
def get_lat_lon(location):
    try:
        if isinstance(location, str):
            geo = geolocator.geocode(location, timeout=10)
            if geo:
                return geo.latitude, geo.longitude
        return None, None
    except GeocoderTimedOut:
        return None, None

# Ensure latitude and longitude columns exist
df["latitude"], df["longitude"] = zip(*df["location"].apply(get_lat_lon))

# Remove rows with missing coordinates
df.dropna(subset=["latitude", "longitude"], inplace=True)

# Create a base map centered at an average location
avg_lat, avg_lon = df["latitude"].mean(), df["longitude"].mean()
m = folium.Map(location=[avg_lat, avg_lon], zoom_start=3)

# Define color mapping for sentiment
sentiment_colors = {
    "POSITIVE": "green",
    "NEGATIVE": "red",
    "NEUTRAL": "gray"
}

# Add markers
for _, row in df.iterrows():
    sentiment = row.get("sentiment_transformers", "NEUTRAL")
    color = sentiment_colors.get(sentiment, "blue")  # Default to blue if not found
    folium.Marker(
        location=[row["latitude"], row["longitude"]],
        popup=f"{row['location']} - Sentiment: {sentiment}",
        icon=folium.Icon(color=color),
    ).add_to(m)

# Save map
map_filename = "conflict_map.html"
m.save(map_filename)

print(f"\n✅ Interactive conflict map saved to {map_filename}")
print("\n✅ Geospatial conflict visualization completed!")
