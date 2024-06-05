import requests
import json
import pandas as pd

# URL of the JSON file
url = "https://raw.githubusercontent.com/IQ-SCM/status-go/959e3703891286853574f4974e87bd53e079e72d/exchanges/exchanges.json"

# Fetching the JSON data from the URL
response = requests.get(url)
data = response.json()

# Print the fetched data
print("Fetched Data:", json.dumps(data, indent=2))

# Extracting all 0x addresses
addresses = []

def extract_addresses(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str) and value.startswith("0x"):
                addresses.append(value)
            else:
                extract_addresses(value)
    elif isinstance(data, list):
        for item in data:
            extract_addresses(item)

extract_addresses(data)

# Print the extracted addresses
print("Extracted Addresses:", addresses)

# Creating a DataFrame and saving to CSV
df = pd.DataFrame(addresses, columns=["addresses"])
csv_path = "0x_addresses.csv"
df.to_csv(csv_path, index=False)

print(f"CSV file saved to {csv_path}")
