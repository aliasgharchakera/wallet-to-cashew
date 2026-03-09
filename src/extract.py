"""Extract data from Budget Bakers Wallet API with pagination."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.models import (
    BBAccount,
    BBBudget,
    BBCategory,
    BBData,
    BBGoal,
    BBLabel,
    BBRecord,
)

BASE_URL = "https://rest.budgetbakers.com/wallet/v1/api"
PAGE_SIZE = 30
DATA_DIR = Path("data/raw")


def _get_token() -> str:
    load_dotenv()
    token = os.getenv("BB_API_TOKEN")
    if not token:
        print("Error: BB_API_TOKEN not set in .env file")
        sys.exit(1)
    return token


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _fetch_paginated(
    endpoint: str,
    token: str,
    key: str,
    *,
    extra_params: dict[str, str] | None = None,
) -> list[dict]:
    """Fetch all pages from a paginated BB API endpoint."""
    all_items: list[dict] = []
    offset = 0
    page_size = 200  # API supports up to 200

    while True:
        params: dict[str, str | int] = {"limit": page_size, "offset": offset}
        if extra_params:
            params.update(extra_params)

        url = f"{BASE_URL}/{endpoint}"
        resp = requests.get(url, headers=_headers(token), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data if isinstance(data, list) else data.get(key, [])
        if isinstance(items, list):
            all_items.extend(items)
        else:
            all_items.append(items)

        # Check for pagination
        next_offset = data.get("nextOffset") if isinstance(data, dict) else None
        if next_offset is None or (isinstance(items, list) and len(items) < page_size):
            break

        offset = next_offset
        time.sleep(0.2)  # Be nice to the API

    return all_items


def _fetch_all_records(token: str) -> list[dict]:
    """Fetch ALL records by iterating in 3-month windows.

    The BB API defaults to 3-month range when only gte is specified.
    We iterate from the earliest possible date forward to today.
    """
    from datetime import datetime, timedelta, timezone

    all_records: list[dict] = []
    seen_ids: set[str] = set()

    # Start before the earliest account (user started March 2024)
    current_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc) + timedelta(days=1)
    window_num = 0

    while current_start < end_date:
        window_num += 1
        start_str = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"    Window {window_num}: from {start_str[:10]}...", end="")

        window_records = _fetch_paginated(
            "records", token, "records",
            extra_params={"recordDate": f"gte.{start_str}"},
        )

        # Deduplicate
        new_count = 0
        for r in window_records:
            rid = r.get("id", "")
            if rid not in seen_ids:
                seen_ids.add(rid)
                all_records.append(r)
                new_count += 1

        print(f" {new_count} new records (total: {len(all_records)})")

        # Move forward ~3 months (API auto-applies 3-month window)
        current_start += timedelta(days=90)
        time.sleep(0.3)

    return all_records


def _fetch_simple(endpoint: str, token: str, key: str | None = None) -> list[dict]:
    """Fetch a non-paginated endpoint. If key is given, extract that field from the wrapper."""
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=_headers(token), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    if key and key in data:
        return data[key]
    if isinstance(data, dict):
        # Try to find the list inside the wrapper
        for v in data.values():
            if isinstance(v, list):
                return v
    return [data]


def _save_raw(name: str, data: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{name}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Saved {len(data)} items to {path}")


def extract() -> BBData:
    """Extract all data from Budget Bakers API."""
    token = _get_token()

    print("Extracting data from Budget Bakers API...")

    # Fetch accounts
    print("\n[1/6] Fetching accounts...")
    raw_accounts = _fetch_simple("accounts", token, key="accounts")
    _save_raw("accounts", raw_accounts)
    accounts = [BBAccount.model_validate(a) for a in raw_accounts]
    print(f"  Found {len(accounts)} accounts")

    # Fetch categories
    print("\n[2/6] Fetching categories...")
    raw_categories = _fetch_simple("categories", token, key="categories")
    _save_raw("categories", raw_categories)
    categories = [BBCategory.model_validate(c) for c in raw_categories]
    print(f"  Found {len(categories)} categories")

    # Fetch labels
    print("\n[3/6] Fetching labels...")
    raw_labels = _fetch_simple("labels", token, key="labels")
    _save_raw("labels", raw_labels)
    labels = [BBLabel.model_validate(l) for l in raw_labels]
    print(f"  Found {len(labels)} labels")

    # Fetch budgets
    print("\n[4/6] Fetching budgets...")
    raw_budgets = _fetch_simple("budgets", token, key="budgets")
    _save_raw("budgets", raw_budgets)
    budgets = [BBBudget.model_validate(b) for b in raw_budgets]
    print(f"  Found {len(budgets)} budgets")

    # Fetch goals
    print("\n[5/6] Fetching goals...")
    raw_goals = _fetch_simple("goals", token, key="goals")
    _save_raw("goals", raw_goals)
    goals = [BBGoal.model_validate(g) for g in raw_goals]
    print(f"  Found {len(goals)} goals")

    # Fetch records (paginated, windowed by date — largest dataset)
    print("\n[6/6] Fetching records (paginated, all date windows)...")
    raw_records = _fetch_all_records(token)
    _save_raw("records", raw_records)
    records = [BBRecord.model_validate(r) for r in raw_records]
    print(f"  Found {len(records)} records total")

    bb_data = BBData(
        accounts=accounts,
        records=records,
        categories=categories,
        labels=labels,
        budgets=budgets,
        goals=goals,
    )

    print(f"\nExtraction complete:")
    print(f"  Accounts:   {len(bb_data.accounts)}")
    print(f"  Categories: {len(bb_data.categories)}")
    print(f"  Labels:     {len(bb_data.labels)}")
    print(f"  Budgets:    {len(bb_data.budgets)}")
    print(f"  Goals:      {len(bb_data.goals)}")
    print(f"  Records:    {len(bb_data.records)}")

    return bb_data


def load_from_raw() -> BBData:
    """Load previously extracted data from raw JSON files."""
    def _load(name: str) -> list[dict]:
        path = DATA_DIR / f"{name}.json"
        if not path.exists():
            print(f"  Warning: {path} not found, returning empty list")
            return []
        with open(path) as f:
            return json.load(f)

    print("Loading data from raw JSON files...")
    return BBData(
        accounts=[BBAccount.model_validate(a) for a in _load("accounts")],
        records=[BBRecord.model_validate(r) for r in _load("records")],
        categories=[BBCategory.model_validate(c) for c in _load("categories")],
        labels=[BBLabel.model_validate(l) for l in _load("labels")],
        budgets=[BBBudget.model_validate(b) for b in _load("budgets")],
        goals=[BBGoal.model_validate(g) for g in _load("goals")],
    )


if __name__ == "__main__":
    extract()
