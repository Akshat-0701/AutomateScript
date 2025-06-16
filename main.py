import requests
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
import csv
from io import StringIO
import argparse
import os

def authenticate_and_run(sec):
    res = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/session',
                        headers={"Content-Type": "application/json"},
                        json={"username": 'ashritha.k@newtonschool.co', "password": sec})
    
    if not res.ok:
        raise Exception("Failed to authenticate")

    token = res.json()['id']
    return token

def update_sheet(gc, token, metabase_card_id, spreadsheet_key, worksheet_name):
    try:
        res = requests.post(f'https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/{metabase_card_id}/query/csv',
                            headers={'X-Metabase-Session': token})
        res.raise_for_status()

        if res.headers.get("Content-Type") == "text/csv":
            reader = csv.reader(StringIO(res.text))
            try:
                header = next(reader)
            except StopIteration:
                print(f"Metabase card {metabase_card_id} returned an empty CSV.")
                header = []
                df = pd.DataFrame(columns=header)
            else:
                df = pd.DataFrame(reader, columns=header)

            sheet = gc.open_by_key(spreadsheet_key)
            worksheet = sheet.worksheet(worksheet_name)

            num_cols = len(df.columns)

            if num_cols > 0:
                max_row = 1000000  # Or a suitable large number
                clear_range = f'A1:{chr(64 + num_cols)}{max_row}' # Define the large clear range
                worksheet.batch_clear([clear_range]) # Use batch_clear with the defined range

            set_with_dataframe(worksheet, df, include_index=False, include_column_header=True, resize=False, row=1, col=1)
            print(f"Updated '{worksheet_name}' with data from card {metabase_card_id}")

        else:
            print(f"Unexpected Content-Type for {metabase_card_id}: {res.headers.get('Content-Type')}")

    except requests.exceptions.RequestException as e:
        print(f"Request Error for {metabase_card_id}: {e}")
        print(f"Status Code: {res.status_code}")
        print(f"Content-Type: {res.headers.get('Content-Type')}")
        print(f"Response Text: {res.text}")
    except gspread.exceptions.APIError as e:
        print(f"Google Sheets API Error for '{worksheet_name}': {e}")
    except Exception as e:
        print(f"Unexpected error for '{worksheet_name}': {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Google Sheets Automation')
    parser.add_argument('--service-account-file', type=str, required=True)
    args = parser.parse_args()

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(args.service_account_file, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sec = os.getenv('ASHRITHA_SECRET_KEY')
    token = authenticate_and_run(sec)
    
    updates = [
            {"metabase_card_id": 7584, "worksheet_name": "Coding", "spreadsheet_key": "1dXvhLhUKnWAQaVW_2ncRlvVspKB2tZnxcFqlYitdwT8"},
            {"metabase_card_id": 7583, "worksheet_name": "MCQ", "spreadsheet_key": "1dXvhLhUKnWAQaVW_2ncRlvVspKB2tZnxcFqlYitdwT8"},
            {"metabase_card_id": 7702, "worksheet_name": "Referrals(7702)", "spreadsheet_key": "1w4oiD9rnazdI1Drz0T2kDGHxKQ-hDDSMriB1nPTKdlo"},
            {"metabase_card_id": 7624, "worksheet_name": "Round-wise details (7624)", "spreadsheet_key": "1w4oiD9rnazdI1Drz0T2kDGHxKQ-hDDSMriB1nPTKdlo"},
            {"metabase_card_id": 7625, "worksheet_name": "Rejection reasons (7625)", "spreadsheet_key": "1w4oiD9rnazdI1Drz0T2kDGHxKQ-hDDSMriB1nPTKdlo"},
            # {"metabase_card_id": 7825, "worksheet_name": "Referrals(7825)", "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
            # {"metabase_card_id": 7823, "worksheet_name": "Round-wise details (7823)", "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
            # {"metabase_card_id": 7826, "worksheet_name": "Rejection reasons (7826)", "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
            # {"metabase_card_id": 7844, "worksheet_name": "All candidates", "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
            # Add other update mappings here
              ]
    for update in updates:
        update_sheet(gc, token, update['metabase_card_id'], update["spreadsheet_key"], update['worksheet_name'])
