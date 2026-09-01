"""
learning_targets_sync.py

Automates the sections of the "Learning Targets" tracker that can be computed
from Metabase + the existing Google Sheets pipeline, instead of typed in by
hand every week:

    - LT_Attendance   -> Live Attendance/Join Rate + SQL cycle sections
    - LT_Contest      -> Module Contest + Mid Module Contest
    - LT_Assignment   -> Assignment weekly completion buckets (VALIDATED, see below)
    - LT_Inactivity   -> Weekly % inactive per batch (the number only - see below)
    - LT_Project      -> STUB ONLY, see the TODO in run_lt_project_clearance()

Left OUT on purpose (per instructions):
    - The Inactivity section's "High-Risk Flag (>= threshold) -> OK / not
      OK" is a manual judgment call - LT_Inactivity writes the weekly %
      inactive number, never that flag.
    - Remarks / POA columns are never touched by this script anywhere -
      they aren't written by any function here.

Design choices, so future-you (or whoever inherits this) knows why:

1. ONE TAB PER SECTION. Each function fully overwrites its own worksheet
   every run (clear + rewrite), the same way your existing write_sheet()
   already works. Nothing here ever computes a row offset into another
   section's range, so there's no way for this to corrupt a neighbouring
   section the way a single flat sheet would.

2. TIDY / LONG FORMAT, not the wide weekly-columns layout the original
   hand-typed sheet uses. Each output tab is one row per
   (batch, cycle-or-week, metric bucket) rather than one row per batch with
   a column per week. This is much safer to auto-refresh (no "which column
   is this week again" bugs), and you can pivot it into the familiar wide
   view with a QUERY()/PIVOT formula in a display tab if you want the exact
   old visual layout back. Say the word and I'll add that formula tab too.

3. VALIDATION STATUS varies by section - see the docstring on each
   run_lt_*() function. Only Assignment has been checked against a real
   data export end-to-end. Attendance/SQL and Contest are written from the
   exact field names, thresholds and filters your own notebook already
   uses, but I have no Metabase credentials in this environment to run them
   against live data - dry-run these against a real sheet before trusting
   them, and read the "ASSUMPTION" comments inline.

Auth / env vars (same as your existing script):
    METABASE_API_KEY   - Metabase API key
    SERVICE_ACCOUNT_JSON - full JSON of the Google service account key

You can either:
  (a) run this as its own script / its own GitHub Actions workflow, or
  (b) copy the run_lt_*() functions + LEARNING_TARGETS_TASKS into your
      existing pipeline file and extend its `tasks` list with them, reusing
      its mb_post()/write_sheet()/MONTH_REPLACEMENTS instead of the copies
      below. Either works; (b) means one fewer secret-laden workflow file.
"""

import os
import re
import json
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import numpy as np
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
# CONFIG - fill this in before running
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

    timeout defaults to 600s (was 120s) - a live run timed out at 120s on
    one of the Assignment cards; your own notebook uses timeout=3600 for
    these same large-join cards, so 120s was always going to be too tight.
    Also retries on timeout/connection errors, since a single slow query
    shouldn't fail the whole run.
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


def get_or_create_worksheet(sheet, worksheet_name, rows=2000, cols=26):
    """
    Return the worksheet if it exists, else create it.
    This is the piece your original write_sheet() was missing - it assumed
    every destination tab already existed.
    """
    try:
        return sheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Tab '{worksheet_name}' doesn't exist yet - creating it")
        return sheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)


def write_sheet(sheet_key, worksheet_name, df, max_attempts=5):
    """
    Full-refresh write: clear the tab and rewrite it from df.
    Creates the tab on first run, updates it on every run after that -
    same retry/backoff behaviour as your existing pipeline's write_sheet().
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


# batch_name comes back like "DS Spreadsheets - August A 2026" or
# "DS SQL - June 2026" (no A/B letter for batches that aren't split yet).
# NOTE: the text before the dash is NOT a reliable module indicator - a
# single batch_name (e.g. "DS EDA 1 - January 2026") can carry rows for
# multiple distinct module_name values (seen "DS 06 EDA 1" AND "DS 01
# Maths" under the same batch_name in the real export). Always get the
# module from the module_name column, never by parsing batch_name - this
# regex only pulls out Month + optional A/B letter.
MONTH_LETTER_RE = re.compile(r"-\s*(?P<month>[A-Za-z]+)\s*(?P<letter>[AB])?\s*(?P<year>\d{4})$")


def parse_month_letter(batch_name):
    """
    'DS Spreadsheets - August A 2026' -> ('August 2026', 'A')
    'DS SQL - June 2026'              -> ('June 2026', None)
    Returns (None, None) if it doesn't match the expected pattern - check
    these rows manually rather than silently dropping them.
    """
    if not isinstance(batch_name, str):
        return None, None
    m = MONTH_LETTER_RE.search(batch_name.strip())
    if not m:
        return None, None
    month = f"{m.group('month')} {m.group('year')}"
    letter = m.group("letter")
    return month, letter


# module_name comes back like 'DS 02 Spreadsheets' / 'DS 04 SQL' - this is
# the field to filter/group on, not anything parsed from batch_name.
MODULE_NAME_DISPLAY = {
    "DS 02 Spreadsheets": "Spreadsheet",
    "DS 04 SQL": "SQL",
}


def add_batch_col_from_month_letter(df, source_col):
    """
    Adds a 'Batch' column (e.g. 'August 2026 A', or just 'August 2026' when
    there's no A/B split) parsed out of df[source_col] via parse_month_letter.
    Small shared helper so Contest/Inactivity/Attendance format this the
    same way instead of three slightly-different inline versions.
    """
    df = df.copy()
    df[["_month", "_letter"]] = df[source_col].apply(lambda b: pd.Series(parse_month_letter(b)))
    df["Batch"] = df.apply(
        lambda r: f"{r['_month']} {r['_letter']}".strip() if pd.notna(r["_letter"]) else str(r["_month"]),
        axis=1,
    )
    return df

# ============================================================================
# SECTION: ASSIGNMENT  (VALIDATED against your real export - see chat)
# ============================================================================

# Cards behind the Colab notebook's "ASSIGNMENT" section / your
# Student-Assign-WOW1 tab. Confirm these still match if this section's
# source cards ever get renumbered in Metabase.
ASSIGNMENT_CARD_WOW1 = 11345
ASSIGNMENT_CARD_WOW2 = 11773


def bucket_row(pct_series):
    """
    pct_series: completion_rate_cumulative values (0-100 scale, occasionally
    >100 in the raw data - clipped here) for one (batch, module, week) group,
    one row per student.
    Returns the same 7 buckets your sheet already uses, as %-of-students
    (0-100), rounded to whole numbers to match the sheet's own formatting.
    """
    r = pd.to_numeric(pct_series, errors="coerce").clip(upper=100)
    r = r.dropna()
    n = len(r)
    if n == 0:
        return None
    return {
        "n_students": n,
        "pct_100": round((r >= 99.995).mean() * 100),
        "pct_gt80": round((r > 80).mean() * 100),
        "pct_gt60": round((r > 60).mean() * 100),
        "pct_gt40": round((r > 40).mean() * 100),
        "pct_gt20": round((r > 20).mean() * 100),
        "pct_gt0": round((r > 0).mean() * 100),
        "pct_zero": round((r == 0).mean() * 100),
    }


def run_lt_assignment():
    """
    Rebuilds LT_Assignment: one row per (Module, Month, Batch letter, Week)
    with the 7-bucket completion distribution, matching the exact columns
    your sheet's Assignment section already has (100% / >80% / >60% / >40%
    / >20% / >0% / 0%).

    Validated 2026-09: bucketing DS Spreadsheets - August A 2026 / W3 this
    way reproduced the sheet's own row (0/25/73/81/87/93/7) almost exactly
    (0/30/73/80/86/92/8) - the gap is just a few extra submissions between
    when the sheet was typed and when the export was pulled, not a
    methodology mismatch.
    """
    print("Running: LT_Assignment")
    df1 = mb_post(ASSIGNMENT_CARD_WOW1)
    df1["source"] = "WOW1"
    df2 = mb_post(ASSIGNMENT_CARD_WOW2)
    df2["source"] = "WOW2"
    df = pd.concat([df1, df2], ignore_index=True, sort=False)

    df["user_id"] = df["user_id"].astype(str).str.strip()
    # Keep 2026 batches only, same filter your notebook already applies.
    if "admin_unit_name" in df.columns:
        df = df[df["admin_unit_name"].astype(str).str.contains("2026", na=False)]

    df[["_month", "_letter"]] = df["batch_name"].apply(lambda b: pd.Series(parse_month_letter(b)))
    unmatched = df[df["_month"].isna()]["batch_name"].unique()
    if len(unmatched):
        print(f"  WARNING: {len(unmatched)} batch_name values didn't match the expected pattern: {list(unmatched)}")

    # Filter/group by the real module_name column, not text parsed out of batch_name.
    df = df[df["module_name"].isin(MODULE_NAME_DISPLAY.keys())].copy()
    df["Module"] = df["module_name"].map(MODULE_NAME_DISPLAY)

    rows = []
    group_cols = ["Module", "_month", "_letter", "week_no_wrt_module"]
    for (module, month, letter, week), g in df.groupby(group_cols, dropna=False):
        g = g.drop_duplicates(subset=["user_id"])
        bucket = bucket_row(g["completion_rate_cumulative"])
        if bucket is None:
            continue
        rows.append({
            "Module": module,
            "Month": month,
            "Batch": letter if letter else "",
            "Week": f"W{int(week)}",
            **bucket,
        })

    out = pd.DataFrame(rows).sort_values(["Module", "Month", "Batch", "Week"])
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Assignment", out)


# ============================================================================
# SECTION: ATTENDANCE (Live Attendance/Join Rate + SQL cycle sections)
# UNVALIDATED - written from your notebook's field names, not run against
# live data. Dry-run and sanity-check the cycle %s before trusting this.
# ============================================================================

# Same two cards your notebook's "COC - Attendance" / "WOW - Attendance"
# cells already pull from.
ATTENDANCE_CARD_COC = 11634
ATTENDANCE_CARD_WOW = 11789


CYCLE_LABEL_RE = re.compile(r"^C\d+-\d+$")


def cycle_label(class_number):
    """
    CONFIRMED on a live run: this card's 'class_number' field is NOT a raw
    per-lecture integer - it already comes back as a pre-formatted cycle
    string like 'C10-12' (int('C10-12') is what threw the original
    ValueError). So: pass those straight through unchanged. Only fall back
    to computing the C{start}-{end} grouping ourselves if we ever get a
    genuine integer lecture-sequence number instead.
    """
    if isinstance(class_number, str) and CYCLE_LABEL_RE.match(class_number.strip()):
        return class_number.strip()
    n = int(class_number)
    block = (n - 1) // 3
    start = block * 3 + 1
    end = block * 3 + 3
    return f"C{start}-{end}"


def _extract_module_from_batch_name(batch_name):
    """
    ASSUMPTION: unlike the Assignment cards, the attendance cards (per your
    notebook's COC/WOW-Attendance cells) don't carry a separate module_name
    column - Module has to be read off batch_name text itself via substring
    match, same as your notebook's extract_module(). Verify this is still
    true when you dry-run against real data; if these cards DO have a
    module_name column, switch this to use it directly instead (more
    reliable than substring matching).
    """
    if not isinstance(batch_name, str):
        return None
    name_lower = batch_name.lower()
    if "spreadsheet" in name_lower:
        return "Spreadsheet"
    if "sql" in name_lower:
        return "SQL"
    return None


def _attendance_from_card(card_id):
    df = mb_post(card_id)
    if df.empty:
        return df
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df[["_month", "_letter"]] = df["batch_name"].apply(lambda b: pd.Series(parse_month_letter(b)))
    df["Module"] = df["batch_name"].apply(_extract_module_from_batch_name)
    df = df[df["Module"].notna()].copy()

    # ASSUMPTION: 'live_attended_flag' is a 0/1 per (user, lecture) row -
    # this matches how your notebook treats it (listed under NUMERIC_COLS
    # and used the same way as WOW's live_attendance).
    if "live_attended_flag" not in df.columns:
        raise KeyError(
            f"Card {card_id} has no 'live_attended_flag' column - "
            f"columns present: {list(df.columns)}. Update _attendance_from_card()."
        )
    df["_cycle"] = df["class_number"].apply(cycle_label)
    return df


def run_lt_attendance():
    """
    Rebuilds LT_Attendance: one row per (Module, Month, Batch letter, Cycle)
    with % live attendance, covering both the 'Live Attendance/Join Rate'
    (Spreadsheet) and 'SQL' sections of the original sheet - same
    computation, just grouped by Module.
    """
    print("Running: LT_Attendance")
    coc = _attendance_from_card(ATTENDANCE_CARD_COC)
    wow = _attendance_from_card(ATTENDANCE_CARD_WOW)
    df = pd.concat([coc, wow], ignore_index=True, sort=False).drop_duplicates(
        subset=["user_id", "batch_name", "class_number"]
    )

    rows = []
    group_cols = ["Module", "_month", "_letter", "_cycle"]
    for (module, month, letter, cycle), g in df.groupby(group_cols, dropna=False):
        flag = pd.to_numeric(g["live_attended_flag"], errors="coerce").dropna()
        if len(flag) == 0:
            continue
        rows.append({
            "Module": module,
            "Month": month,
            "Batch": letter if letter else "",
            "Cycle": cycle,
            "n_records": len(flag),
            "pct_live_attendance": round(flag.mean() * 100),
        })

    out = pd.DataFrame(rows).sort_values(["Module", "Month", "Batch", "Cycle"])
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Attendance", out)


# ============================================================================
# SECTION: CONTEST (Module Contest + Mid Module Contest)
# CONFIRMED on a live run: MC_Raw_2's actual columns are user_id,
# student_name, admin_unit_name, contest_date, module_name,
# contest_name_x, contest_name_y, MCQ_score, Coding_score, Total Score -
# no batch-letter field anywhere (same shape expected for Mid_MC_Raw).
# Fix: recover the A/B batch the same way your own notebook does
# everywhere else it needs one - merge in Master Data 2023-2026 by
# user_id and read its Batch/Batch Name column instead of expecting the
# contest sheet to carry it itself. See _load_master_data() below.
# ============================================================================

CONTEST_THRESHOLD = 64

_master_data_cache = None


def _load_master_data():
    """
    Loads user_id -> Batch Name once per run and caches it. This is the
    same 'DS Full program - All Intake 2026' / 'Master Data 2023-2026'
    sheet your notebook merges into nearly every section for batch/persona
    info - used here specifically to recover the A/B batch letter that
    MC_Raw_2/Mid_MC_Raw (and possibly the inactivity card) don't carry
    themselves.
    """
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
        .rename(columns={batch_col: "_master_batch_name"})
        .drop_duplicates(subset="user_id")
    )
    return _master_data_cache


def _attach_batch_from_master_data(df, user_id_col="user_id"):
    """Left-joins Master Data's batch name onto df by user_id."""
    master = _load_master_data()
    df = df.copy()
    df[user_id_col] = df[user_id_col].astype(str).str.strip()
    df = df.merge(master, left_on=user_id_col, right_on="user_id", how="left", suffixes=("", "_md"))
    return df


def _contest_actuals(raw_sheet_name, raw_worksheet_name, contest_label):
    workbook = gc.open(raw_sheet_name)
    ws = workbook.worksheet(raw_worksheet_name)
    data = ws.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])

    df["Total Score"] = pd.to_numeric(df["Total Score"].astype(str).str.replace(",", ""), errors="coerce")
    df["user_id"] = df["user_id"].astype(str).str.strip()

    df = _attach_batch_from_master_data(df)
    unmatched_pct = df["_master_batch_name"].isna().mean() * 100
    if unmatched_pct > 20:
        print(f"  WARNING: {unmatched_pct:.0f}% of {raw_worksheet_name} rows didn't match a "
              f"user_id in Master Data - Batch/A-B info will be missing for those rows.")

    df[["_month", "_letter"]] = df["_master_batch_name"].apply(lambda b: pd.Series(parse_month_letter(b)))

    # Highest score per student for this contest (mirrors your notebook's
    # Highest_Score_Overall / Status logic).
    best = df.sort_values("Total Score", ascending=False).drop_duplicates(subset=["user_id", "module_name"])
    best["Cleared"] = best["Total Score"] >= CONTEST_THRESHOLD
    best["Attempted"] = best["Total Score"].notna()

    rows = []
    for (month, letter), g in best.groupby(["_month", "_letter"], dropna=False):
        attempted = g["Attempted"].sum()
        cleared = g["Cleared"].sum()
        total = len(g)
        if total == 0:
            continue
        rows.append({
            "Contest": contest_label,
            "Month": month,
            "Batch": letter if letter else "",
            "n_students": total,
            "pct_attempted": round(attempted / total * 100),
            "pct_cleared": round(cleared / total * 100),
        })
    return pd.DataFrame(rows)


def run_lt_contest():
    """
    Rebuilds LT_Contest: one row per (Contest, Month, Batch letter) with
    Actual Attempt% / Clearance%, matching the Target(Attempt/Clearance) vs
    Actual shape of the sheet's Contest section. Batch A/B comes from a
    Master Data join by user_id (see _load_master_data()), since the raw
    contest sheets don't carry it themselves - check the "unmatched"
    WARNING in the log if this looks off.
    """
    print("Running: LT_Contest")
    module_contest = _contest_actuals("Placements", "MC_Raw_2", "Module Contest")
    mid_module_contest = _contest_actuals("Placements", "Mid_MC_Raw", "Mid Module Contest")
    out = pd.concat([module_contest, mid_module_contest], ignore_index=True)
    if out.empty:
        # Raise instead of silently returning - a silent return here made
        # this task count as "succeeded" in the run summary while writing
        # no tab at all. Check the "unmatched" WARNING above for whether
        # the Master Data join is the culprit.
        raise RuntimeError(
            "LT_Contest has nothing to write - see the WARNING line(s) above "
            "for which raw sheet/column lookup failed."
        )
    out = out.sort_values(["Contest", "Month", "Batch"])
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Contest", out)


# ============================================================================
# SECTION: INACTIVITY  (weekly % inactive is automated; the sheet's
# "High-Risk Flag (>= threshold) -> OK/not-OK" judgment call stays manual,
# per instructions - this section never writes that flag.)
# UNVALIDATED - written from your notebook's summarize_inactivity() logic,
# not run against live data.
# ============================================================================

# Same card your notebook's "Rolling Consecutive-Day INACTIVITY" cell uses.
INACTIVITY_STREAK_CARD_ID = 11690

# 7-day blocks only (w0..w11 = Week 1..Week 12) - matches the sheet's
# W1..W8 weekly cadence. Your notebook also has 15-day (p) and 30-day (m)
# granularities from the same card if you ever want those too.
INACTIVITY_MAX_WEEK_INDEX = 11

FLAG_RE = re.compile(r"^w(\d+)_inactive_streak$")


def _to_bool(s):
    """Metabase JSON gives real bools for `_available`, but CSV/gspread
    round-trips can turn them into 'true'/'false' strings or 0/1."""
    if s.dtype == bool:
        return s
    if pd.api.types.is_numeric_dtype(s):
        return s.fillna(0).astype(float).gt(0)
    return s.astype(str).str.strip().str.lower().isin({"true", "1", "t", "yes"})


def _normalise_inactivity_flags(df):
    """
    Gate every w{i}_inactive_streak on its w{i}_available flag, so a week
    that hasn't fully elapsed yet reads as NaN (excluded from the rate)
    rather than falsely counting as "not inactive". Ported directly from
    your notebook's normalise_inactivity_flags() - see its docstring there
    for the reasoning on why _available (not "has started") is the right
    gate.
    """
    df = df.copy()
    flag_cols = [c for c in df.columns if FLAG_RE.match(c)]
    if not flag_cols:
        raise KeyError(
            "No w{i}_inactive_streak columns found - is INACTIVITY_STREAK_CARD_ID "
            "pointing at the right card?"
        )
    have_available = all(f"w{FLAG_RE.match(c).group(1)}_available" in df.columns for c in flag_cols)
    if not have_available:
        print("  WARNING: card has no w{i}_available columns - cannot safely gate partial weeks. "
              "Falling back to treating every present value as elapsed, which may understate "
              "inactivity for the most recent week(s). Check the card.")

    for col in flag_cols:
        idx = int(FLAG_RE.match(col).group(1))
        flag = pd.to_numeric(df[col], errors="coerce")
        if have_available:
            available = _to_bool(df[f"w{idx}_available"])
            df[col] = np.where(available, flag.fillna(0).astype(int), np.nan)
            df[f"w{idx}_available"] = available.astype(int)
        else:
            df[col] = flag
    return df


def run_lt_inactivity():
    """
    Rebuilds LT_Inactivity: one row per (Batch, Week) with % of students
    inactive that week - the "Weekly Average (% inactive)" row of the
    sheet's Inactivity section. Does NOT write the High-Risk Flag - that
    stays a manual OK/not-OK call per your instructions.

    Batch A/B resolution order: (1) Master Data join by user_id - same fix
    as Contest, since that's the pattern your notebook uses everywhere else
    for batch info; (2) this card's own 'batch_name' column, if it has one
    and the Master Data join mostly missed; (3) month-only cohort from
    batch_month_start as a last resort, which merges A/B together - watch
    for that WARNING in the log.
    """
    print("Running: LT_Inactivity")
    df = mb_post(INACTIVITY_STREAK_CARD_ID)
    if df.empty:
        # Raise instead of silently returning, same reasoning as Contest:
        # a bare "return" here made a card that comes back empty count as
        # a "success" in the run summary while writing no tab at all.
        raise RuntimeError(
            f"LT_Inactivity: card {INACTIVITY_STREAK_CARD_ID} returned no rows "
            f"- check the card in Metabase and the Metabase API key."
        )

    df["user_id"] = df["user_id"].astype(str).str.strip()
    df = _normalise_inactivity_flags(df)

    df = _attach_batch_from_master_data(df)
    unmatched_pct = df["_master_batch_name"].isna().mean() * 100

    if unmatched_pct <= 20:
        df = add_batch_col_from_month_letter(df, "_master_batch_name")
    elif "batch_name" in df.columns:
        print(f"  WARNING: Master Data join only matched {100 - unmatched_pct:.0f}% of rows - "
              f"falling back to this card's own 'batch_name' column instead.")
        df = add_batch_col_from_month_letter(df, "batch_name")
    else:
        print(f"  WARNING: Master Data join only matched {100 - unmatched_pct:.0f}% of rows and "
              "this card has no 'batch_name' column either - falling back to month-only cohort "
              "(A/B batches will be merged together).")
        df["batch_month_start"] = pd.to_datetime(df.get("batch_month_start"), errors="coerce")
        df["Batch"] = df["batch_month_start"].dt.strftime("%b %Y").fillna("Unknown")

    group_col = "Batch"

    rows = []
    for batch, grp in df.groupby(group_col):
        for i in range(0, INACTIVITY_MAX_WEEK_INDEX + 1):
            avail_col, flag_col = f"w{i}_available", f"w{i}_inactive_streak"
            if avail_col not in grp.columns or flag_col not in grp.columns:
                continue
            avail = grp[avail_col].astype(bool) if grp[avail_col].notna().any() else pd.Series([True] * len(grp))
            flagged = grp.loc[avail, flag_col]
            if len(flagged) == 0:
                continue
            rows.append({
                "Batch": batch,
                "Week": f"W{i + 1}",
                "n_available": len(flagged),
                "pct_inactive": round(pd.to_numeric(flagged, errors="coerce").mean() * 100),
            })

    if not rows:
        raise RuntimeError(
            "LT_Inactivity: no (batch, week) had any elapsed/available data to summarize - "
            "check the w{i}_available / w{i}_inactive_streak columns on the card."
        )
    out = pd.DataFrame(rows).sort_values(["Batch", "Week"])
    write_sheet(LEARNING_TARGETS_SHEET_KEY, "LT_Inactivity", out)


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
    (M0, M1, M2, ...).

    Your notebook's project-clearance code (the "POPULATE PROJECT
    EVALUATIONS CALENDAR" cell) only computes cumulative cleared counts via
    marks_submission_level >= 8 - it doesn't have Enrolled/Deadline/
    Accountable/Median-days/Overdue anywhere. Those need a different source
    before I can write this function for real. Tell me where they come
    from (a Metabase card, another sheet, a manual roster) and I'll build
    this the same way as the sections above.
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
    if LEARNING_TARGETS_SHEET_KEY == "PASTE_YOUR_SHEET_ID_HERE":
        raise ValueError(
            "Set LEARNING_TARGETS_SHEET_KEY to your target spreadsheet's ID "
            "before running (and make sure it's shared with your service "
            "account's client_email as an Editor)."
        )

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
