import requests
import pandas as pd
import re
from datetime import datetime

# --- Constants ---
today_date = datetime.now().strftime("%Y-%m-%d")
EYE_URL = 'https://api-v2.7signal.com/eyes/agents'
ORG_URL = 'https://api-v2.7signal.com/organizations'
AUTH_URL = 'https://api-v2.7signal.com/oauth2/token'
EXCEL_PATH = "C:/Python Path/Customer_Data.xlsx"
AGENT_OUTPUT_FILE = f"agent_version_summary.csv"
ANDROID_OUTPUT_FILE = f"android_version_summary.csv"

# Read customer details
customers_df = pd.read_excel(EXCEL_PATH, engine='openpyxl')


# --- Authentication ---
def authenticate(client_id, client_secret):
    """Authenticate and return the access token."""
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
        response = requests.post(AUTH_URL, data=auth_data, headers=auth_headers, timeout=10)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.RequestException as e:
        print(f"❌ Authentication failed: {e}")
        return None


# --- Fetch Organization Data ---
def fetch_organization_data(token):
    """Fetch organization data and return a DataFrame with organization ID and name."""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    try:
        response = requests.get(ORG_URL, headers=headers, timeout=10)
        response.raise_for_status()
        org_data = response.json()
        return pd.DataFrame([
            {'organizationId': org.get('id'), 'account_name': org.get('name')}
            for org in org_data.get('results', [])
            if 'id' in org and 'name' in org
        ])
    except requests.RequestException as e:
        print(f"❌ Failed to fetch organization data: {e}")
        return pd.DataFrame()


# --- Fetch Eye Data ---
def fetch_eye_data(token, organizationId):
    """Fetch eye data for an organization."""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    try:
        response = requests.get(EYE_URL, headers=headers, params={"organizationId": organizationId}, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        #print(f"❌ Failed to fetch eye data: {e}")
        return None


# --- Process Data ---
def process_data(token, organizationId, account_name, version_prefix):
    """Fetch and process eye data for specified version prefixes."""
    eye_data = fetch_eye_data(token, organizationId)
    if not eye_data:
        return pd.DataFrame()

    eye_df = pd.json_normalize(eye_data.get('results', []))
    if eye_df.empty or 'lastAgentVersion' not in eye_df.columns:
        return pd.DataFrame()

    # Filter rows based on prefix
    filtered_df = eye_df[eye_df['lastAgentVersion'].astype(str).str.startswith(version_prefix)]
    if filtered_df.empty:
        return pd.DataFrame()

    version_counts = filtered_df['lastAgentVersion'].value_counts()
    total_versions = version_counts.sum()
    version_percentage = (version_counts / total_versions * 100).round(1).to_dict()
    version_percentage = {k: f"{v:.1f}%" for k, v in version_percentage.items()}

    row = {'account_name': account_name}
    row.update(version_percentage)
    return pd.DataFrame([row])


# --- Sorting Functions ---
def version_sort_key(version):
    match = re.match(r'v(\d+)\.(\d+)\.(\d+)', version)
    if match:
        return (0, tuple(int(part) for part in match.groups()))
    match = re.match(r'(\d+)\.(\d+)\.(\d+)', version)
    if match:
        return tuple(int(part) for part in match.groups())
    return (2, version)


def sort_dataframe(df, prefix, sort_key=None):
    """Sort DataFrame columns based on a prefix and sorting key."""
    if df.empty:
        print("⚠️ Warning: DataFrame is empty, skipping sorting.")
        return df

    columns_filtered = [col for col in df.columns if col.startswith(prefix)]
    if sort_key:
        columns_filtered = sorted(columns_filtered, key=sort_key, reverse=True)
    else:
        columns_filtered = sorted(columns_filtered, reverse=True)

    final_columns = ['account_name'] + columns_filtered
    return df[final_columns]


def concatenate_non_zero(row):
    """Concatenate non-zero percentage columns."""
    return ' | '.join(
        f"{col}, {row[col]}"
        for col in row.index if col != 'account_name' and row[col] != '0.0%'
    )


# --- Main Script ---
def main():
    # Create empty DataFrames to hold all data across customers
    all_agent_data = []
    all_android_data = []

    # Loop through each customer
    for _, customer_row in customers_df.iterrows():
        client_id = customer_row['client_id']
        client_secret = customer_row['client_secret']

        if pd.isna(client_id) or pd.isna(client_secret):
            print(f"❌ Missing client_id or client_secret for customer: {customer_row['account_name']}. Skipping...")
            continue

        # Authenticate and fetch organization data
        token = authenticate(client_id, client_secret)
        if not token:
            print(f"❌ Failed to authenticate for customer: {customer_row['account_name']}. Skipping...")
            continue

        organization_df = fetch_organization_data(token)
        if organization_df.empty:
            print(f"❌ No organization data found for customer: {customer_row['account_name']}. Skipping...")
            continue

        # Process each organization under this customer
        for _, org_row in organization_df.iterrows():
            print(f"🔄 Processing organization: {org_row['account_name']}")
            agent_data = process_data(token, org_row['organizationId'], org_row['account_name'], 'v')
            android_data = process_data(token, org_row['organizationId'], org_row['account_name'], ('4', '5'))

            if not agent_data.empty:
                all_agent_data.append(agent_data)
            if not android_data.empty:
                all_android_data.append(android_data)

    # Combine and Save Agent Data
    if all_agent_data:
        agent_df = pd.concat(all_agent_data, ignore_index=True).fillna("0.0%")
        agent_df = sort_dataframe(agent_df, 'v', version_sort_key)
        agent_df['agent_version_data'] = agent_df.apply(concatenate_non_zero, axis=1)
        agent_df.to_csv(AGENT_OUTPUT_FILE, index=False)
        print(f"✅ All Agent data saved to {AGENT_OUTPUT_FILE}")
    else:
        print("⚠️ No valid Agent data found to save.")

    # Combine and Save Android Data
    if all_android_data:
        android_df = pd.concat(all_android_data, ignore_index=True).fillna("0.0%")
        android_df = sort_dataframe(android_df, ('4', '5'))
        android_df['android_version_data'] = android_df.apply(concatenate_non_zero, axis=1)
        android_df.to_csv(ANDROID_OUTPUT_FILE, index=False)
        print(f"✅ All Android data saved to {ANDROID_OUTPUT_FILE}")
    else:
        print("⚠️ No valid Android data found to save.")



if __name__ == "__main__":
    main()
