"""
learning_targets_sync.py

Keeps the "Learning Targets" tracker's underlying data fresh by dumping raw,
lightly-cleaned Metabase data into one tab per section:

    - LT_Assignment   -> Assignment WOW1/WOW2 cards, student x module x week rows
    - LT_Attendance   -> Lecture-level attendance (COC + WOW), student x lecture rows
    - LT_Contest      -> Module Contest + Mid Module Contest, student x attempt rows
    - LT_Inactivity   -> 7/15/30-day inactivity streak flags, student x period rows
    - LT_Project      -> STUB ONLY, see the TODO in run_lt_project_clearance()

DESIGN: RAW DUMP, NOT PRE-AGGREGATED. Earlier versions tried to compute the
sheet's exact weekly/cycle Actual% numbers in Python (bucketing, cycle
grouping, monthly rollups). That kept breaking because batch-naming is
genuinely inconsistent across sheets in this org - three different formats
turned up across three different sheets while building this (see chat).
Rather than keep guessing regexes, each section here just dumps its
cleaned raw/student-level data plus whatever raw batch label Master Data
has for that student (unparsed, as-is) - build the exact weekly/cycle view
in the spreadsheet itself with QUERY()/pivot/lookup formulas, the same way
several of your existing production tabs (Projects-1,
Assignments_questions_bucket) already work. This is also just more robust
to auto-refresh unattended: nothing here depends on a batch name matching
a particular text pattern.

Left OUT on purpose (per instructions):
    - The Inactivity section's "High-Risk Flag (>= threshold) -> OK / not
      OK" is a manual judgment call - not written here.
    - Remarks / POA columns are never touched by this script anywhere.

One tab per section, full clear+rewrite every run (same as your existing
write_sheet() pattern) - nothing here computes a row offset into another
section's range, so there's no way for this to corrupt a neighbouring
section the way a single flat sheet would.

Auth / env vars (same as your existing script):
    METABASE_API_KEY     - Metabase API key
    SERVICE_ACCOUNT_JSON  - full JSON of the Google service account key

You can either:
  (a) run this as its own script / its own GitHub Actions workflow, or
  (b) copy the run_lt_*() functions + LEARNING_TARGETS_TASKS into your
      existing pipeline file and extend its `tasks` list with them, reusing
      its mb_post()/write_sheet() instead of the copies below.
"""

import os
import json
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# ============================================================================
# AUTH  (identical pattern to your existing pipeline script)
# ============================================================================

METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

if not METABASE_API_KEY or not service_account_json:
    raise ValueError("Missing environment variables. Check GitHub secrets.")

service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

METABASE_URL = "https://metabase-lierhfgoeiwhr.newtonschool.co"
METABASE_HEADERS = {"Content-Type": "application/json", "X-API-Key": METABASE_API_KEY}

# ============================================================================
# CONFIG
# ============================================================================

# The spreadsheet all LT_* tabs get written into. Must already be shared
# with your service account's client_email (found in SERVICE_ACCOUNT_JSON)
# as an Editor - the script only creates/updates worksheets (tabs) inside
# it, not the spreadsheet itself.
LEARNING_TARGETS_SHEET_KEY = "1qbZKVJ3Q_QnbQZ5Vzo8k-9f8Fo4Yu2Rsr4emBKUeHNM"

# ============================================================================
# SHARED HELPERS
# ============================================================================

def mb_post(card_id, timeout=600, max_attempts=3):
    """
    POST to a Metabase card's query endpoint and return parsed JSON rows.

    timeout defaults to 600s (a live run timed out at 120s on one of the
    Assignment cards; your own notebook uses timeout=3600 for these same
    large-join cards). Retries on timeout/connection errors, since a single
    slow query shouldn't fail the whole run.
    """
    url = f"{METABASE_URL}/api/card/{card_id}/query/json"
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(url, headers=METABASE_HEADERS, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if not data:
                print(f"Warning: card {card_id} returned no rows")
            return pd.DataFrame(data)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_attempts:
                print(f"  Card {card_id} attempt {attempt}/{max_attempts} failed ({e}) - retrying...")
                time.sleep(10 * attempt)
            else:
                raise


def get_or_create_worksheet(sheet, worksheet_name, rows=5000, cols=100):
    """Return the worksheet if it exists, else create it."""
    try:
        return sheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Tab '{worksheet_name}' doesn't exist yet - creating it")
        return sheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)


def write_sheet(sheet_key, worksheet_name, df, max_attempts=5):
    """
    Full-refresh write: clear the tab and rewrite it from df. Creates the
    tab on first run, updates it on every run after that.
    """
    print(f"Updating tab: {worksheet_name} ({len(df)} rows)")
    for attempt in range(1, max_attempts + 1):
        try:
            time.sleep(2)  # rate-limit protection
            sheet = gc.open_by_key(sheet_key)
            ws = get_or_create_worksheet(sheet, worksheet_name)
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            print(f"  OK: {worksheet_name}")
            return
        except gspread.exceptions.APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "Quota exceeded" in str(e):
                wait = 60 * attempt
                print(f"  Rate limit hit, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Attempt {attempt} failed for {worksheet_name}: {e}")
                if attempt < max_attempts:
                    time.sleep(20)
                else:
                    raise
        except Exception as e:
            print(f"  Attempt {attempt} failed for {worksheet_name}: {e}")
            if attempt < max_attempts:
                time.sleep(20)
            else:
                raise


def require_rows(df, label):
    """Fail loudly instead of writing (or silently skipping) an empty tab."""
    if df is None or df.empty:
        raise RuntimeError(f"{label}: nothing to write after cleaning/filtering - check the log above.")
    return df


# module_name comes back like 'DS 02 Spreadsheets' / 'DS 04 SQL' on the
# Assignment cards - the field to filter/tag on, not anything parsed out
# of a batch name.
MODULE_NAME_DISPLAY = {
    "DS 02 Spreadsheets": "Spreadsheet",
    "DS 04 SQL": "SQL",
}


def tag_module_from_batch_name(batch_name):
    """
    Best-effort Module tag for cards that only have batch_name (no separate
    module_name column, e.g. the attendance cards) - substring match, same
    approach as your notebook's extract_module(). Informational only now;
    nothing downstream depends on this being exactly right.
    """
    if not isinstance(batch_name, str):
        return None
    name_lower = batch_name.lower()
    if "spreadsheet" in name_lower:
        return "Spreadsheet"
    if "sql" in name_lower:
        return "SQL"
    return None


# ============================================================================
# MASTER DATA - attached as a raw, UNPARSED lookup column on every section
# below, so you have a batch label to join/QUERY against in Sheets even
# though its exact text format varies by source (confirmed: at least three
# different batch-naming conventions exist across this org's sheets, so
# parsing it in Python isn't reliable - do that matching in Sheets where
# you can see the real values).
# ============================================================================

_master_data_cache = None


def _load_master_data():
    """Loads user_id -> raw Batch/Batch Name once per run and caches it."""
    global _master_data_cache
    if _master_data_cache is not None:
        return _master_data_cache

    ws = gc.open("DS Full program - All Intake 2026").worksheet("Master Data 2023-2026")
    data = ws.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.rename(columns={"User ID ": "user_id"})
    df["user_id"] = df["user_id"].astype(str).str.strip()

    batch_col = "Batch Name" if "Batch Name" in df.columns else ("Batch" if "Batch" in df.columns else None)
    if batch_col is None:
        raise KeyError(
            f"Master Data 2023-2026 has neither 'Batch Name' nor 'Batch' column - "
            f"columns present: {list(df.columns)}. Update _load_master_data()."
        )

    _master_data_cache = (
        df[["user_id", batch_col]]
        .rename(columns={batch_col: "master_batch_raw"})
        .drop_duplicates(subset="user_id")
    )
    return _master_data_cache


def attach_master_batch(df, user_id_col="user_id"):
    """Left-joins Master Data's raw batch label onto df by user_id."""
    master = _load_master_data()
    df = df.copy()
    df[user_id_col] = df[user_id_col].astype(str).str.strip()
    df = df.merge(master, left_on=user_id_col, right_on="user_id", how="left", suffixes=("", "_md"))
    unmatched_pct = df["master_batch_raw"].isna().mean() * 100
    if unmatched_pct > 20:
        print(f"  NOTE: {unmatched_pct:.0f}% of rows didn't match a user_id in Master Data - "
              f"master_batch_raw will be blank for those rows.")
    return df


# ============================================================================
# SECTION: ASSIGNMENT
# Cards behind the Colab notebook's "ASSIGNMENT" section / your
# Student-Assign-WOW1 tab.
# ============================================================================

ASSIGNMENT_CARD_WOW1 = 11345
ASSIGNMENT_CARD_WOW2 = 11773


def run_lt_assignment():
    """
    Raw dump: one row per (student, batch, module, week) with the fields
    Metabase already computes (completion_rate_cumulative, attempt_rate_
    cumulative, open_rate_cumulative, etc.) - build the sheet's 100%/>80%/
    >60%/>40%/>20%/>0%/0% distribution with a QUERY()/COUNTIFS formula
    against this tab, grouped however you want by module_name/batch_name/
    week_no_wrt_module.
    """
    print("Running: LT_Assignment")
    df1 = mb_post(ASSIGNMENT_CARD_WOW1)
    df1["source"] = "WOW1"
    df2 = mb_post(ASSIGNMENT_CARD_WOW2)
    df2["source"] = "WOW2"
    df = pd.concat([df1, df2], ignore_index=True, sort=False)

    df["user_id"] = df["user_id"].astype(str).str.strip()
    if "admin_unit_name" in df.columns:
        df = df[df["admin_unit_name"].astype(str).str.contains("2026", na=False)]
    if "module_name" in df.columns:
        df = df[df["module_name"].isin(MODULE_NAME_DISPLAY.keys())].copy()
        df["Module"] = df["module_name"].map(MODULE_NAME_DISPLAY)

    df = attach_master_batch(df)
    df = require_rows(df, "LT_Assignment")
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Assignment", df)


# ============================================================================
# SECTION: ATTENDANCE
# Same two cards your notebook's "COC - Attendance" / "WOW - Attendance"
# cells pull from.
# ============================================================================

ATTENDANCE_CARD_COC = 11634
ATTENDANCE_CARD_WOW = 11789


def run_lt_attendance():
    """
    Raw dump: one row per (student, batch, lecture) from both attendance
    cards, filtered to Spreadsheet/SQL batches, deduped across the two
    cards. Build the sheet's per-cycle % live attendance with a QUERY()
    grouped by batch_name and whatever cycle/class-number field the card
    provides (confirmed live: some cards return this pre-formatted, e.g.
    'C10-12', rather than a plain integer - dumped here as-is either way).
    """
    print("Running: LT_Attendance")
    frames = []
    for card_id in (ATTENDANCE_CARD_COC, ATTENDANCE_CARD_WOW):
        df = mb_post(card_id)
        if df.empty:
            continue
        df["user_id"] = df["user_id"].astype(str).str.strip()
        if "batch_name" in df.columns:
            df["Module"] = df["batch_name"].apply(tag_module_from_batch_name)
            df = df[df["Module"].notna()].copy()
        frames.append(df)

    if not frames:
        raise RuntimeError("LT_Attendance: both attendance cards returned no rows.")

    df = pd.concat(frames, ignore_index=True, sort=False)
    dedup_cols = [c for c in ["user_id", "batch_name", "class_number", "lecture_id"] if c in df.columns]
    if dedup_cols:
        df = df.drop_duplicates(subset=dedup_cols)

    df = attach_master_batch(df)
    df = require_rows(df, "LT_Attendance")
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Attendance", df)


# ============================================================================
# SECTION: CONTEST (Module Contest + Mid Module Contest)
# ============================================================================

CONTEST_THRESHOLD = 64


def _contest_raw(raw_sheet_name, raw_worksheet_name, contest_label):
    workbook = gc.open(raw_sheet_name)
    ws = workbook.worksheet(raw_worksheet_name)
    data = ws.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])

    df["Total Score"] = pd.to_numeric(df["Total Score"].astype(str).str.replace(",", ""), errors="coerce")
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df["Contest"] = contest_label
    df["Cleared"] = df["Total Score"] >= CONTEST_THRESHOLD
    return df


def run_lt_contest():
    """
    Raw dump: every contest attempt row from MC_Raw_2 and Mid_MC_Raw, each
    tagged with which contest it's from and a Cleared (score >= 64) flag,
    plus the raw Master Data batch label. Build the sheet's Attempt%/
    Clearance% Actuals with a QUERY() grouped however your batch labels
    actually need slicing - this data doesn't assume any particular A/B
    naming convention.
    """
    print("Running: LT_Contest")
    module_contest = _contest_raw("Placements", "MC_Raw_2", "Module Contest")
    mid_module_contest = _contest_raw("Placements", "Mid_MC_Raw", "Mid Module Contest")
    df = pd.concat([module_contest, mid_module_contest], ignore_index=True, sort=False)

    df = attach_master_batch(df)
    df = require_rows(df, "LT_Contest")
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Contest", df)


# ============================================================================
# SECTION: INACTIVITY (weekly % inactive - the number only; the sheet's
# High-Risk OK/not-OK judgment call stays manual, untouched here)
# ============================================================================

INACTIVITY_STREAK_CARD_ID = 11690


def run_lt_inactivity():
    """
    Raw dump: the student-level 7-day inactivity streak flags (w0_inactive_
    streak..w11_inactive_streak) straight off the card, plus the raw Master
    Data batch label. Build the sheet's "Weekly Average (% inactive)" row
    with a QUERY()/AVERAGEIF per (batch, week) against this tab - note the
    card's own w{i}_available columns (if present) should gate the average
    so a week that hasn't fully elapsed yet isn't counted as "not inactive".
    """
    print("Running: LT_Inactivity")
    df = mb_post(INACTIVITY_STREAK_CARD_ID)
    df = require_rows(df, "LT_Inactivity (card returned no rows)")

    df["user_id"] = df["user_id"].astype(str).str.strip()
    df = attach_master_batch(df)
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Inactivity", df)


# ============================================================================
# SECTION: PROJECT CLEARANCE - STUB, NOT IMPLEMENTED
# ============================================================================

def run_lt_project_clearance():
    """
    NOT IMPLEMENTED YET.

    The sheet's Project section (Spreadsheet Project Clearance / SQL Project
    Clearance) needs: Currently Enrolled, Deadline, Accountable, Total
    Submit, Total Cleared, Median Days for cleared learners, Submitted not
    Cleared, Overdue, Overdue not cleared - per batch, per month cohort
    (M0, M1, M2, ...). Your notebook's project-clearance code only computes
    cumulative cleared counts via marks_submission_level >= 8 - it doesn't
    have Enrolled/Deadline/Accountable/Median-days/Overdue anywhere. Tell
    me where those come from (a Metabase card, another sheet, a manual
    roster) and I'll build this the same way as the sections above.
    """
    raise NotImplementedError(
        "Project Clearance source data not identified yet - see docstring."
    )


# ============================================================================
# MAIN
# ============================================================================

LEARNING_TARGETS_TASKS = [
    ("LT: Assignment", run_lt_assignment),
    ("LT: Attendance", run_lt_attendance),
    ("LT: Contest", run_lt_contest),
    ("LT: Inactivity", run_lt_inactivity),
    # ("LT: Project Clearance", run_lt_project_clearance),  # not ready yet
]

if __name__ == "__main__":
    print("Starting Learning Targets sync...")
    print(f"Start: {datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%d-%b-%Y %H:%M:%S IST')}\n")

    ok, failed = 0, []
    for name, fn in LEARNING_TARGETS_TASKS:
        try:
            fn()
            ok += 1
        except Exception as e:
            print(f"FAILED: {name}: {e}")
            traceback.print_exc()
            failed.append(name)

    print("\n" + "=" * 60)
    print(f"Done: {ok}/{len(LEARNING_TARGETS_TASKS)} succeeded")
    if failed:
        print(f"Failed: {', '.join(failed)}")
    print(f"End: {datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%d-%b-%Y %H:%M:%S IST')}")
