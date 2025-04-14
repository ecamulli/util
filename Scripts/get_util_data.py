import json
import requests
import csv
import os
import pandas as pd
from tqdm import tqdm  # For progress bar
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import openpyxl

today_date = datetime.now().strftime("%Y-%m-%d")


# API URLs
USERS_URL = 'https://api-v2.7signal.com/users'
EYE_URL = 'https://api-v2.7signal.com/eyes'
ORG_URL = 'https://api-v2.7signal.com/organizations'
AUTH_URL = 'https://api-v2.7signal.com/oauth2/token'

# Excel file containing customer details
EXCEL_PATH = "C:/Python Path/Customer_Data.xlsx"
MASTER_OUTPUT_FILE = "utilization_data"

# Read the customer details from the spreadsheet
customers_df = pd.read_excel(EXCEL_PATH, engine='openpyxl')

# Function to fetch and process data for each customer
def fetch_customer_data(client_id, client_secret, organizationId, account_name):
    # Authentication data
    auth_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    auth_headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        # Step 1: Get authentication token
        token_response = requests.post(AUTH_URL, data=auth_data, headers=auth_headers, timeout=10)
        token_response.raise_for_status()
        token = token_response.json().get("access_token")
        # print(f"Token response: {token_response}"),
        # print(f"Access Token: {token}")



        if not token:
            print(f"No access token received for client_id: {client_id}")
            return pd.DataFrame()

        # Set up headers for API requests
        headers_eyes = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}"
        }

        # Fetch user, eye, and organization data
        responses = {
            'users': requests.get(USERS_URL, headers=headers_eyes, timeout=20),
            'eye': requests.get(f"{EYE_URL}?organizationId={organizationId}", headers=headers_eyes, timeout=40),
            'org': requests.get(ORG_URL, headers=headers_eyes, timeout=20),
        }


        for key, response in responses.items():
            response.raise_for_status()
           # print(f"Raw JSON response from {key.upper()} API:")
           # print(json.dumps(response.json(), indent=4))  # Pretty print JSON response to the screen

        
        # Process user data
        users_data = responses['users'].json().get("results", [])
        user_df = pd.json_normalize(users_data) if users_data else pd.DataFrame()

        # Handle the 'lastLogin' and 'email' columns safely
        if 'lastLogin' in user_df.columns:
            user_df['Last_Login'] = user_df['lastLogin'].apply(format_timestamp)
        else:
            user_df['Last_Login'] = None


        # Ensure neccessary columns exist with default values
        user_df['lastLogin'] = user_df.get('lastLogin', 'N/A')
        user_df['firstName'] = user_df.get('firstName', 'N/A')
        user_df['lastName'] = user_df.get('lastName', 'N/A')

        # Create 'Last_Login_Name' and 'Last_Login_and_Name'
        user_df['Last_Login_Name'] = user_df['firstName'] + ' ' + user_df['lastName']
        user_df['Last_Login_and_Name'] = user_df['Last_Login_Name'] + ' - ' + user_df['Last_Login']

        # Explicit datetime parsing
        user_df['Last_Login'] = pd.to_datetime(
            user_df['Last_Login'], 
            format='%b %d %Y',  # Adjust if needed
            errors='coerce')

        # Format timestamps and create combined login column if data exists
        if not user_df.empty:
            user_df['Last_Login'] = user_df['lastLogin'].apply(format_timestamp)
            user_df['Last_Login_Name'] = user_df['firstName'] + ' ' + user_df['lastName']
            user_df['Last_Login_and_Name'] = user_df['Last_Login_Name'] + ' - ' + user_df['Last_Login']
            
            user_df.drop(columns=['id', 'role.id', 'auth0Id', 'lastLogin'], inplace=True, errors='ignore')

        
        #Remove 7SIGNAL employees from list
        if 'email' in user_df.columns:
            user_df = user_df[~user_df['email'].str.contains("@7signal.com", na=False)]

        # Sort the DataFrame by 'Last_Login' chronologically
        user_df['Last_Login'] = pd.to_datetime(user_df['Last_Login'], errors='coerce')
        user_df = user_df.sort_values(by='Last_Login', ascending=False)

        # Combine multiple rows for 'Last_Login_and_Name' into one cell
        user_df['Last_Login_and_Name'] = user_df.groupby(lambda x: 0)['Last_Login_and_Name'].transform(
            lambda x: ' | '.join(x.dropna().unique())
        )

        # Drop duplicate rows since data has been merged into a single cell
        user_df = user_df.drop_duplicates(subset=['Last_Login_and_Name'], keep='first')



        # Process eye and organization data
        eye_data = responses['eye'].json()
        org_data = responses['org'].json()

        flattened_eye_data = {
            "agents_organizationName": eye_data.get("agents", {}).get("organizationName", "N/A"),
            "agents_deviceCount": eye_data.get("agents", {}).get("deviceCount", 0),
            "agents_licenseSummary_packageName": eye_data.get("agents", {}).get("licenseSummary", {}).get("packageName", "N/A"),
            "agents_licenseSummary_totalLicenses": eye_data.get("agents", {}).get("licenseSummary", {}).get("totalLicenses", 0),
            "agents_licenseSummary_usedLicenses": eye_data.get("agents", {}).get("licenseSummary", {}).get("usedLicenses", 0),
            "agents_licenseSummary_freeLicenses": eye_data.get("agents", {}).get("licenseSummary", {}).get("freeLicenses", 0),
            "sensors_deviceCount": eye_data.get("sensors", {}).get("deviceCount", 0),
            "sensors_deviceStatusSummary_offline": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("offline", 0),
            "sensors_deviceStatusSummary_stopped": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("stopped", 0),
            "sensors_deviceStatusSummary_idle": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("idle", 0),
            "sensors_deviceStatusSummary_active": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("active", 0),
            "sensors_deviceStatusSummary_maintenance": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("maintenance", 0),
            "sensors_modelSummary_eye6300": eye_data.get("sensors", {}).get("modelSummary", {}).get("eye 6300", 0),
            "sensors_modelSummary_eye6200": eye_data.get("sensors", {}).get("modelSummary", {}).get("eye 6200", 0),
            "sensors_modelSummary_eye2200": eye_data.get("sensors", {}).get("modelSummary", {}).get("eye 2200", 0),
            "sensors_modelSummary_eye250": eye_data.get("sensors", {}).get("modelSummary", {}).get("eye 250", 0),
        }
        combined_data = []
        for result in org_data.get("results", []):
            merged_data = flattened_eye_data.copy()
            merged_data.update({
                "pagination_perPage": org_data.get("pagination", {}).get("perPage", "N/A"),
                "pagination_page": org_data.get("pagination", {}).get("page", "N/A"),
                "pagination_total": org_data.get("pagination", {}).get("total", "N/A"),
                "pagination_pages": org_data.get("pagination", {}).get("pages", "N/A"),
                "result_connection_id": result.get("connection", {}).get("id", "N/A"),
                "result_id": result.get("id", "N/A"),
                "result_name": result.get("name", "N/A"),
                "result_mobileEyeOrgCode": result.get("mobileEyeOrgCode", "N/A"),
                "result_isSuspended": result.get("isSuspended", "N/A")
            })
            combined_data.append(merged_data)

        eye_df = pd.DataFrame(combined_data)
        return pd.concat([user_df.reset_index(drop=True), eye_df.reset_index(drop=True)], axis=1)

        
    except requests.RequestException as e:
        print(f"Error fetching data for: {account_name}")
        return pd.DataFrame()

# Utility function to format timestamp
def format_timestamp(iso_timestamp):
    try:
        if not isinstance(iso_timestamp, str):
            return None  # or use a default string like 'N/A'
        dt_object = datetime.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt_object.strftime("%b %d %Y")
    except ValueError:
        return iso_timestamp

# Utility function to flatten eye data properly
def flatten_eye_data(eye_data, organizationId):
    """ Flattens nested eye data JSON response into a single dictionary """
    return {
        "organizationId": organizationId,
        "agents_organizationName": eye_data.get("agents", {}).get("organizationName", "N/A"),
        "agents_deviceCount": eye_data.get("agents", {}).get("deviceCount", 0),
        "agents_licenseSummary_packageName": eye_data.get("agents", {}).get("licenseSummary", {}).get("packageName", "N/A"),
        "agents_licenseSummary_totalLicenses": eye_data.get("agents", {}).get("licenseSummary", {}).get("totalLicenses", 0),
        "agents_licenseSummary_usedLicenses": eye_data.get("agents", {}).get("licenseSummary", {}).get("usedLicenses", 0),
        "agents_licenseSummary_freeLicenses": eye_data.get("agents", {}).get("licenseSummary", {}).get("freeLicenses", 0),
        "agents_platformSummary_windows": eye_data.get("agents", {}).get("platformSummary", {}).get("windows", 0),
        "agents_platformSummary_android": eye_data.get("agents", {}).get("platformSummary", {}).get("android", 0),
        "sensors_deviceCount": eye_data.get("sensors", {}).get("deviceCount", 0),
        "sensors_deviceStatusSummary_offline": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("offline", 0),
        "sensors_deviceStatusSummary_stopped": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("stopped", 0),
        "sensors_deviceStatusSummary_idle": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("idle", 0),
        "sensors_deviceStatusSummary_active": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("active", 0),
        "sensors_deviceStatusSummary_maintenance": eye_data.get("sensors", {}).get("deviceStatusSummary", {}).get("maintenance", 0),
        "sensors_modelSummary_eye250": eye_data.get("sensors", {}).get("modelSummary", {}).get("eye 250", 0),
        "sensors_modelSummary_eye6300": eye_data.get("sensors", {}).get("modelSummary", {}).get("eye 6300", 0),
        "sensors_modelSummary_eye2200": eye_data.get("sensors", {}).get("modelSummary", {}).get("eye 2200", 0),
    }
    

# Utility function to prepare combined data
def prepare_combined_data(flattened_eye_data, org_data):
    combined_data = []
    for result in org_data.get("results", []):
        merged_data = flattened_eye_data.copy()
        merged_data.update({
            "pagination_perPage": org_data.get("pagination", {}).get("perPage", "N/A"),
            "pagination_page": org_data.get("pagination", {}).get("page", "N/A"),
            "result_id": result.get("id", "N/A"),
            "result_name": result.get("name", "N/A")
        })
        combined_data.append(merged_data)
    return combined_data



# Main loop to process all customers
data_list = []
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_customer = {
        executor.submit(fetch_customer_data, row['client_id'], row['client_secret'], row['organizationId'], row['account_name']): row
        for _, row in customers_df.iterrows()
    }
    for future in tqdm(as_completed(future_to_customer), total=len(future_to_customer), desc="Processing Customers"):
        customer_data = future.result()
        if not customer_data.empty:
            data_list.append(customer_data)



# Combine all collected customer data into a single DataFrame
if data_list:
    master_df = pd.concat(data_list, ignore_index=True)

    
    # Drop specified columns
    columns_to_remove = ['firstName', 'lastName', 'email', 'Last_Login', 'lastLogin', 'Last_Login_Name', 'agents_organizationName', 'agents_licenseSummary_packageName',
    'result_mobileEyeOrgCode', 'result_id', 'result_connection_id', 'result_isSuspended',
    'pagination_perPage', 'pagination_page', 'pagination_pages', 'pagination_total']
    master_df = master_df.drop(columns=[col for col in columns_to_remove if col in master_df.columns], errors='ignore')

    # Move 'Last_Login_and_Name' column to the end
    if 'Last_Login_and_Name' in master_df.columns:
        last_column = master_df.pop('Last_Login_and_Name')
        master_df['Last_Login_and_Name'] = last_column

    # Make 'result_name' the first column if it exists
    if 'result_name' in master_df.columns:
        result_name_column = master_df.pop('result_name')
        master_df.insert(0, 'result_name', result_name_column)

    
    # Drop unwanted columns first
    columns_to_remove += ['roleId', 'roleKey']
    master_df = master_df.drop(columns=[col for col in columns_to_remove if col in master_df.columns], errors='ignore')


    # Rename all columns
    master_df.columns = ['Account Name', 'Agents Deployed', 'Agents Purchased', 'Agents Active', 'Agents Free', 
    'Sensors Total', 'Sensors Offline', 'Sensors Stopped', 'Sensors Idle',  'Sensors Active', 'Sensors Maintenance',
    'Sensors 6300', 'Sensors 6200', 'Sensors 2200', 'Sensors 250', 'Last Login' ]


    # Create a new directory to save the files
    output_dir = 'History'
    os.makedirs(output_dir, exist_ok=True)

    # Save the DataFrame to two new CSV files in the new directory
    master_df.to_csv(os.path.join(output_dir, f'{MASTER_OUTPUT_FILE}_{today_date}.csv'), index=False, quoting=csv.QUOTE_ALL)
    master_df.to_csv(os.path.join(output_dir, f"{MASTER_OUTPUT_FILE}.csv"), index=False, quoting=csv.QUOTE_ALL)

    

else:
    print("No data to save.")
