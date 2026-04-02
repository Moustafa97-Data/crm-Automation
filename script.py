import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import os
import json

def run_script():

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    
    creds_dict = json.loads(os.environ['GOOGLE_CREDS'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

    client = gspread.authorize(creds)

    sheet = client.open("Automated CRM Sheets").worksheet("Leads")

    # ===== Read =====
    data = sheet.get_all_values()
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)

    df.columns = (
        df.columns
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
        .str.lower()
    )

    # ===== Vectorized ID + Created ===== 💣
    now_ts = int(datetime.now().timestamp())
    today_str = datetime.now().strftime("%Y-%m-%d")

    df['lead_id'] = df.apply(
        lambda r: f"L-{now_ts}-{r.name+2}" if not r['lead_id'] and r['email'] else r['lead_id'],
        axis=1
    )

    df['created_at'] = df.apply(
        lambda r: today_str if not r['created_at'] and r['email'] else r['created_at'],
        axis=1
    )

    # ===== Score =====
    df['last_activity'] = pd.to_datetime(df['last_activity'], errors='coerce')
    df['days_inactive'] = (datetime.now() - df['last_activity']).dt.days

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

    # ===== 💣 Update مرة واحدة (أهم تحسين) =====
    update_data = df[['lead_id', 'score', 'created_at']].values.tolist()

    # A, G, H
    for i, row in enumerate(update_data, start=2):
        sheet.update(f"A{i}:A{i}", [[row[0]]])
        sheet.update(f"G{i}:G{i}", [[int(row[1])]])
        sheet.update(f"H{i}:H{i}", [[row[2]]])

    print("Optimized update done 🚀")


if __name__ == "__main__":
    run_script()
