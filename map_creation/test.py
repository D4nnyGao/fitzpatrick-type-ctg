import os
import googlemaps
from dotenv import load_dotenv
import json

# --- 1. SETUP ---
# Load variables from your .env file
load_dotenv()

# Securely load your API key
API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
if not API_KEY:
    raise ValueError("❌ Google Maps API key not found. Make sure it's in your .env file.")

# Initialize the Google Maps client
try:
    gmaps = googlemaps.Client(key=API_KEY)
    print("✅ Google Maps client initialized successfully.")
except Exception as e:
    print(f"🔥 Error initializing Google Maps client: {e}")
    exit()

# --- 2. DEFINE TEST QUERY ---
# An unambiguous query for a well-known place
test_query = "The White House, Washington DC"
print(f"[*] Testing Places API with query: '{test_query}'")

# --- 3. MAKE THE API CALL & PRINT RESULTS ---
try:
    # Make the Text Search request
    response = gmaps.places(query=test_query)

    if response and response.get('results'):
        print("\n✅ API call successful! Found results.")
        
        # Get the first and most relevant result
        place = response['results'][0]
        
        # Extract key information
        place_name = place.get('name')
        address = place.get('formatted_address')
        location = place.get('geometry', {}).get('location', {})
        latitude = location.get('lat')
        longitude = location.get('lng')

        print("-" * 30)
        print(f"   Official Name: {place_name}")
        print(f"         Address: {address}")
        print(f"      Coordinates: Lat: {latitude}, Lng: {longitude}")
        print("-" * 30)
        
        # Optional: Print the full raw response to see all available data
        # print("\nFull API Response:")
        # print(json.dumps(response, indent=2))

    else:
        print("\n⚠️ API call was successful, but no results were found for the query.")

except googlemaps.exceptions.ApiError as e:
    print(f"\n🔥 An API error occurred: {e}")
except Exception as e:
    print(f"\n🔥 An unexpected error occurred: {e}")