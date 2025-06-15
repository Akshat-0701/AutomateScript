#!/usr/bin/env python3
"""
Pull several Metabase cards as CSV and write them into Google-Sheets
worksheets.  Designed for GitHub Actions but also runnable locally.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import sys
import time
from io import StringIO
from typing import Optional

import gspread
import pandas as pd
import requests
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────────────────── Config ────────────────────────────────
METABASE_URL = "https://metabase-lierhfgoeiwhr.newtonschool.co"
METABASE_USERNAME = os.getenv("MB_USERNAME", "ashritha.k@newtonschool.co")
METABASE_PASSWORD = os.getenv("ASHRITHA_SECRET_KEY")

HTTP_TIMEOUT = 30                    # seconds per request
MAX_RETRIES = 5                      # tries per Metabase call
INITIAL_BACKOFF = 1.5                # seconds
DELAY_BETWEEN_CARDS = 2              # seconds between successive cards

UPDATES = [
    {"metabase_card_id": 7584, "worksheet_name": "Coding",
     "spreadsheet_key": "1dXvhLhUKnWAQaVW_2ncRlvVspKB2tZnxcFqlYitdwT8"},
    {"metabase_card_id": 7583, "worksheet_name": "MCQ",
     "spreadsheet_key": "1dXvhLhUKnWAQaVW_2ncRlvVspKB2tZnxcFqlYitdwT8"},
    {"metabase_card_id": 7702, "worksheet_name": "Referrals(7702)",
     "spreadsheet_key": "1w4oiD9rnazdI1Drz0T2kDGHxKQ-hDDSMriB1nPTKdlo"},
    {"metabase_card_id": 7624, "worksheet_name": "Round-wise details (7624)",
     "spreadsheet_key": "1w4oiD9rnazdI1Drz0T2kDGHxKQ-hDDSMriB1nPTKdlo"},
    {"metabase_card_id": 7625, "worksheet_name": "Rejection reasons (7625)",
     "spreadsheet_key": "1w4oiD9rnazdI1Drz0T2kDGHxKQ-hDDSMriB1nPTKdlo"},
    {"metabase_card_id": 7825, "worksheet_name": "Referrals(7825)",
     "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
    {"metabase_card_id": 7823, "worksheet_name": "Round-wise details (7823)",
     "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
    {"metabase_card_id": 7826, "worksheet_name": "Rejection reasons (7826)",
     "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
    {"metabase_card_id": 7844, "worksheet_name": "All candidates",
     "spreadsheet_key": "1r5hP96xuyNsGfmNyzkSC_nEsuRxB7vaiceXqK-bvbxw"},
]

# ───────────────────────────── Utilities ───────────────────────────────


def col_to_a1(n: int) -> str:
    """Convert 1-based column index to A1 column label (e.g. 1→A, 27→AA)."""
    label = ""
    while n:
        n, rem = divmod(n - 1, 26)
        label = chr(65 + rem) + label
    return label


session = requests.Session()          # shared HTTP session


def post_with_retry(path: str, **kwargs) -> requests.Response:
    """POST to Metabase with retries and exponential back-off."""
    url = f"{METABASE_URL}{path}"
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, timeout=HTTP_TIMEOUT, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            jitter = random.random()
            sleep_time = backoff + jitter
            print(f"⚠️  {e} — retry {attempt}/{MAX_RETRIES} in "
                  f"{sleep_time:.1f}s", file=sys.stderr)
            time.sleep(sleep_time)
            backoff *= 2  # exponential


# ───────────────────────────── Core logic ──────────────────────────────
def authenticate() -> str:
    if not METABASE_PASSWORD:
        raise RuntimeError("Environment variable ASHRITHA_SECRET_KEY is empty")
    payload = {"username": METABASE_USERNAME, "password": METABASE_PASSWORD}
    res = post_with_retry("/api/session",
                          json=payload,
                          headers={"Content-Type": "application/json"})
    token = res.json()["id"]
    session.headers.update({"X-Metabase-Session": token})
    return token


def update_sheet(gc, card_id: int, spreadsheet_key: str, worksheet_name: str):
    """Download one Metabase card and put it in the target worksheet."""
    print(f"→ Updating card {card_id} → {worksheet_name}")

    # --- 1) download CSV -------------------------------------------------
    res: Optional[requests.Response] = None
    try:
        res = post_with_retry(f"/api/card/{card_id}/query/csv")
    except Exception as e:
        print(f"❌  Card {card_id}: request failed after retries: {e}",
              file=sys.stderr)
        return

    if not res.headers.get("Content-Type", "").startswith("text/csv"):
        print(f"❌  Card {card_id}: unexpected Content-Type "
              f"{res.headers.get('Content-Type')}", file=sys.stderr)
        return

    reader = csv.reader(StringIO(res.text))
    try:
        header = next(reader)
    except StopIteration:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(reader, columns=header)

    # --- 2) write to Google Sheet ---------------------------------------
    try:
        sheet = gc.open_by_key(spreadsheet_key)
        ws = sheet.worksheet(worksheet_name)

        if len(df.columns):
            clear_range = f"A1:{col_to_a1(len(df.columns))}"
            ws.batch_clear([clear_range])

        set_with_dataframe(ws, df, include_index=False,
                           include_column_header=True,
                           resize=True, row=1, col=1)

        print(f"✓ Done ({len(df)} rows, {len(df.columns)} cols)")
    except gspread.exceptions.APIError as e:
        print(f"❌  Google Sheets API error for '{worksheet_name}': {e}",
              file=sys.stderr)


# ──────────────────────────────── main ─────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Metabase → Google-Sheets")
    parser.add_argument("--service-account-file", required=True)
    args = parser.parse_args()

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(args.service_account_file,
                                                  scopes=scopes)
    gc = gspread.authorize(creds)

    authenticate()  # sets header in global `session`

    for entry in UPDATES:
        update_sheet(gc,
                     entry["metabase_card_id"],
                     entry["spreadsheet_key"],
                     entry["worksheet_name"])
        time.sleep(DELAY_BETWEEN_CARDS)


if __name__ == "__main__":
    main()
