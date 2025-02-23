import os
from dotenv import load_dotenv

# Load Environment Variables from the .env file
env_path = os.path.join(os.path.dirname(__file__), "../config/.env")
load_dotenv(env_path)

# Print the values (for debugging)
print("✅ GDELT API Key:", os.getenv("GDELT_API_KEY") or "❌ NOT FOUND")
print("✅ MongoDB URI:", os.getenv("MONGODB_URI") or "❌ NOT FOUND")
print("✅ OpenAI API Key:", os.getenv("OPENAI_API_KEY") or "❌ NOT FOUND")
