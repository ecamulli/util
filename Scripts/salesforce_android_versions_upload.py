import requests
import csv
from datetime import datetime
from urllib.parse import quote  # Import to encode query strings safely

# Salesforce credentials and URLs
salesforce_url = 'https://login.salesforce.com/services/oauth2/token'
my_salesforce_url = 'https://7signal.my.salesforce.com'
api_version = 'v56.0'

# Authentication data
auth_data = {
    "client_id": "3MVG9A2kN3Bn17huYjGcBgLUdAql0zHKfoE6NB6lzR3TcMBMhNtvMkdgiEREtGAXdvlbrOIne35Rid.2BxQhI",
    "client_secret": "B7FA81D4CAED43712BB9E002E012F963FD8A1915ED9B9581720E35648F3FD542",
    "grant_type": "password",
    "username": "eric.camulli@7signal.com",
    "password": "UJjGkgT^FELz3@9CdgpJcV2Hrag884IxX6Q0OhlG"
}

auth_headers = {
    "Accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded"
}

# Step 1: Authenticate to get the access token
token_exch_response = requests.post(salesforce_url, data=auth_data, headers=auth_headers)

if token_exch_response.status_code == 200:
    token_exch_json_response = token_exch_response.json()
    token = token_exch_response.json().get("access_token")

    if token:
        print("Authentication to Salesforce successful!")

        # Headers for subsequent requests
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        # Step 2: Read the master file and match account names
        consolidated_file = 'android_version_summary.csv'
        account_updates = {}

        # Get today's date in ISO-8601 format
        today_date = datetime.now().date().isoformat()

        with open(consolidated_file, mode='r') as file:
            reader = csv.DictReader(file)

            for row in reader:
                account_name = row['account_name']
                android_versions_raw = row['android_version_data'].strip()  


                # Initialize the payload for updating Salesforce
                update_payload = {
                    "Last_Utilization_Audit__c": today_date  # Add today's date to the payload
                }

                
                # Process Android Versions
                if android_versions_raw:
                    update_payload = {
                        "Android_App_Version_s__c": android_versions_raw  # Send the raw string directly
                    }
                    account_updates[account_name] = update_payload
                

                # Only store updates if there are valid fields
                if update_payload:
                    account_updates[account_name] = update_payload

        # Escape single quotes and clean account names
        escaped_account_names = [name.replace("'", "''").strip() for name in account_updates.keys()]

        # Check if the query is too long and split into smaller chunks if needed
        batch_size = 200
        chunks = [escaped_account_names[i:i + batch_size] for i in range(0, len(escaped_account_names), batch_size)]

        # Process each chunk
        for chunk in chunks:
            query = "SELECT Id, Name FROM Account WHERE Name IN ('" + "','".join(chunk) + "')"
            url = f"{my_salesforce_url}/services/data/{api_version}/query?q={quote(query)}"

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()

                for record in data['records']:
                    account_id = record['Id']
                    account_name = record['Name']

                    if account_name in account_updates:
                        update_url = f"{my_salesforce_url}/services/data/{api_version}/sobjects/Account/{account_id}"
                        update_payload = account_updates[account_name]

                        update_response = requests.patch(update_url, json=update_payload, headers=headers)

                        if update_response.status_code == 204:
                            print(f"Successfully updated: {account_name} with fields {update_payload.keys()}")
                        else:
                            print(f"Failed to update Account: {account_name} - {update_response.text}")
            else:
                print(f"Error fetching Salesforce accounts: {response.status_code}, {response.text}")
else:
    print(f"Error fetching authentication token: {token_exch_response.status_code}, {token_exch_response.text}")