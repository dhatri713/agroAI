import requests
import csv
import io
from datetime import datetime
import time  # To add delay between requests

# API details
BASE_URL = "https://www.data.gov.in/resource/cef25fe2-9231-4128-8aec-2c948fedd43f"  # Corrected endpoint
HEADERS = {
    "Authorization": "579b464db66ec23bdd000001a4b0ca459fed463d6a453bc7fb9ec510",  # Your API key
    "Content-Type": "application/json"
}

# Filters
states = ["TELANGANA", "ANDHRA PRADESH"]  # States to iterate over
years = ["2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"]
months = [
    "1", "2", "3", "4", "5", "6",
    "7", "8", "9", "10", "11", "12"
]

# Pagination setup
limit = 1000  # Adjust based on API's maximum allowed
output_data = []  # Store combined data

# Iterate over states, years, and months
for state in states:
    for year in years:
        for month in months:
            offset = 0
            while True:
                # Construct query parameters
                print(f"Currently at {state}, {year}, {month}, offset: {offset}")
                params = {
                    "offset": offset,
                    "limit": limit,
                    "filters[StateName.keyword]": state,
                    "filters[year.keyword]": str(year),
                    "filters[month.keyword]": month,
                }

                # Make the API request
                response = requests.get(BASE_URL, headers=HEADERS, params=params)

                # Check response status
                if response.status_code == 200:
                    if response.content:  # Check for non-empty content
                        csv_content = response.content.decode("utf-8")
                        csv_reader = csv.reader(io.StringIO(csv_content))

                        # Skip the header for subsequent requests
                        if offset == 0 and len(output_data) == 0:
                            output_data.extend(list(csv_reader))  # Include header for the first batch
                        else:
                            output_data.extend(list(csv_reader)[1:])  # Skip header for additional batches

                        # Check if fewer records are returned than the limit (end of data)
                        if len(csv_content.splitlines()) < limit:
                            print(f"End of data for {state}, {year}, {month}")
                            break
                        else:
                            offset += limit
                    else:
                        print(f"No data for {state}, {year}, {month}. Moving to next.")
                        break
                else:
                    print(f"Error: {response.status_code}, {response.text}")
                    break

                # Add a delay between requests to avoid rate limiting or too many requests
                time.sleep(2)

# Save combined results to a CSV file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"combined_data_{timestamp}.csv"
with open(output_file, "w", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(output_data)

print(f"Data saved to {output_file}")
