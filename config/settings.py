import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    MONGO_URI =mongodb+srv://Lawofsupremacism:InstanceofSecurity1147@cluster0.4qtz1.mongodb.net/?retryWrites=true&w=m$
    GDELT_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

settings = Settings()
