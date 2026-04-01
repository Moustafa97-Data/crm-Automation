import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import os
import json

def run_script():

    # ===== Connect =====
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = json.loads(os.environ['GOOGLE_CREDS'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

    client = gspread.authorize(creds)

    spreadsheet = client.open("Automated CRM Sheets")
    sheet = spreadsheet.worksheet("Leads")

    # ===== Read =====
    data = sheet.get_all_values()
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)

    # تنظيف الأعمدة
    df.columns = (
        df.columns
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
        .str.lower()
    )

    # ===== Handle IDs + Created At =====
    id_values = []
    created_values = []

    for i, row in df.iterrows():
        lead_id = row['lead_id']
        email = row['email']

        if not lead_id and email:
            new_id = f"L-{int(datetime.now().timestamp())}-{i+2}"
        else:
            new_id = lead_id

        id_values.append([new_id])

        if not row['created_at'] and email:
            created_values.append([datetime.now().strftime("%Y-%m-%d")])
        else:
            created_values.append([row['created_at']])

    # ===== Score =====
    df['last_activity'] = pd.to_datetime(df['last_activity'], errors='coerce')

    today = datetime.now()
    df['days_inactive'] = (today - df['last_activity']).dt.days

    def calculate_score(row):
        score = 0

        if row['status'] == 'new':
            score += 10
        elif row['status'] == 'contacted':
            score += 20
        elif row['status'] == 'qualified':
            score += 40
        elif row['status'] == 'converted':
            score += 60

        if pd.notna(row['days_inactive']):
            if row['days_inactive'] > 30:
                score -= 20
            elif row['days_inactive'] > 15:
                score -= 10

        return max(score, 0)

    df['score'] = df.apply(calculate_score, axis=1)

    score_values = [[int(val)] for val in df['score']]

    # ===== Update =====
    sheet.update(id_values, f'A2:A{len(id_values)+1}')
    sheet.update(created_values, f'H2:H{len(created_values)+1}')
    sheet.update(score_values, f'G2:G{len(score_values)+1}')

if __name__ == "__main__":
    run_script()
