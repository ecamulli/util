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
    "password": "%jDdumZae9YE3JZBXo01ZNoUfbbIfR4w1tHNoSY"
}

auth_headers = {
    "Accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded"
}

# Optional helper to avoid repeated try/except blocks
def safe_int_parse(value, field_label, account_name):
    try:
        return int(float(value.strip()))
    except ValueError:
        print(f"Skipping invalid {field_label} for Account: {account_name} - Value: {value}")
        return None

# Step 1: Authenticate to get the access token
token_exch_response = requests.post(salesforce_url, data=auth_data, headers=auth_headers)

if token_exch_response.status_code == 200:
    token_exch_json_response = token_exch_response.json()
    token = token_exch_json_response.get("access_token")

    if token:
        print("Authentication to Salesforce successful!")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        consolidated_file = 'Output/utilization_data.csv'
        account_updates = {}
        today_date = datetime.now().date().isoformat()

        with open(consolidated_file, mode='r') as file:
            reader = csv.DictReader(file)

            for row in reader:
                account_name = row['Account Name']
                update_payload = {
                    "Last_Utilization_Audit__c": today_date
                }

                # Fields to map and parse
                field_mappings = {
                    "Agents Active": "Agents_Active__c",
                    "Agents Deployed": "Agents_Deployed__c",
                    "Sensors Active": "Sensors_Active__c",
                    "Sensors Total": "Sensors_Deployed__c"
                }

                for csv_field, sf_field in field_mappings.items():
                    raw_value = row.get(csv_field, "").strip()
                    if raw_value:
                        parsed_value = safe_int_parse(raw_value, csv_field, account_name)
                        if parsed_value is not None:
                            update_payload[sf_field] = parsed_value

                # Additional sensor fields
                sensor_fields = {
                    "Sensors 2200": "Sensors_2200__c",
                    "Sensors 250": "Sensors_250__c",
                    "Sensors 6200": "Sensors_6200__c",
                    "Sensors 6300": "Sensors_6300__c",
                    "Sensors Stopped": "Sensors_Stopped__c",
                    "Sensors Idle": "Sensors_Idle__c",
                    "Sensors Maint.": "Sensors_Maint__c",
                    "Sensors Offline": "Sensors_Offline__c"
                }

                for csv_field, sf_field in sensor_fields.items():
                    raw_value = row.get(csv_field, "").strip()
                    if raw_value:
                        parsed_value = safe_int_parse(raw_value, csv_field, account_name)
                        if parsed_value is not None:
                            update_payload[sf_field] = parsed_value

                # Last Login and Name
                last_login_and_name_raw = row.get('Last Login', "").strip()
                if last_login_and_name_raw:
                    update_payload["Last_Login_and_Name__c"] = last_login_and_name_raw

                if update_payload:
                    account_updates[account_name] = update_payload

        escaped_account_names = [name.replace("'", "''").strip() for name in account_updates.keys()]
        batch_size = 200
        chunks = [escaped_account_names[i:i + batch_size] for i in range(0, len(escaped_account_names), batch_size)]

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
