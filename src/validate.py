"""Validate the migrated Cashew database and generate a migration report."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from src.models import BBData, CashewData


def validate(bb_data: BBData, cashew_data: CashewData, db_path: Path) -> bool:
    """Validate migration and print a report. Returns True if all checks pass."""
    print("\n" + "=" * 60)
    print("MIGRATION VALIDATION REPORT")
    print("=" * 60)

    issues: list[str] = []

    # ── 1. Entity count comparison ──
    print("\n--- Entity Counts ---")
    checks = [
        ("Accounts → Wallets", len(bb_data.accounts), len(cashew_data.wallets)),
        ("Goals → Objectives", len(bb_data.goals), len(cashew_data.objectives)),
    ]
    for label, source, target in checks:
        status = "OK" if source == target else "MISMATCH"
        print(f"  {label}: {source} → {target} [{status}]")
        if source != target:
            issues.append(f"{label} count mismatch: {source} vs {target}")

    # Records → Transactions
    print(f"  Records → Transactions: {len(bb_data.records)} → {len(cashew_data.transactions)}", end="")
    if len(bb_data.records) == len(cashew_data.transactions):
        print(" [OK]")
    else:
        diff = len(bb_data.records) - len(cashew_data.transactions)
        print(f" [DIFF: {diff}]")
        issues.append(f"Record count diff: {diff}")

    # Categories (Cashew will have more due to defaults + subcategories)
    print(f"  BB Categories: {len(bb_data.categories)} → Cashew Categories: {len(cashew_data.categories)} (includes defaults + subcategories)")

    # Budgets (Cashew will have more due to label-derived budgets)
    label_budgets = sum(1 for b in cashew_data.budgets if b.addedTransactionsOnly)
    regular_budgets = len(cashew_data.budgets) - label_budgets
    print(f"  BB Budgets: {len(bb_data.budgets)} → Cashew Budgets: {len(cashew_data.budgets)} ({regular_budgets} regular + {label_budgets} from labels)")

    # ── 2. Database integrity checks ──
    print("\n--- Database Integrity ---")
    conn = sqlite3.connect(str(db_path))
    try:
        # Check schema version
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        print(f"  Schema version: {version} [{'OK' if version == 46 else 'WRONG'}]")
        if version != 46:
            issues.append(f"Schema version is {version}, expected 46")

        # Check table existence
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        expected_tables = {
            "wallets", "categories", "transactions", "budgets",
            "objectives", "app_settings",
        }
        missing = expected_tables - tables
        if missing:
            print(f"  Missing tables: {missing}")
            issues.append(f"Missing tables: {missing}")
        else:
            print(f"  All required tables present [OK]")

        # Check row counts in DB match in-memory data
        for table, expected in [
            ("wallets", len(cashew_data.wallets)),
            ("categories", len(cashew_data.categories)),
            ("transactions", len(cashew_data.transactions)),
            ("budgets", len(cashew_data.budgets)),
            ("objectives", len(cashew_data.objectives)),
        ]:
            actual = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            status = "OK" if actual == expected else "MISMATCH"
            print(f"  {table}: {actual} rows [{status}]")
            if actual != expected:
                issues.append(f"{table} row count: expected {expected}, got {actual}")

        # ── 3. Referential integrity ──
        print("\n--- Referential Integrity ---")

        # Check all transaction category_fk references exist
        orphan_cats = conn.execute("""
            SELECT COUNT(*) FROM transactions t
            WHERE t.category_fk NOT IN (SELECT category_pk FROM categories)
        """).fetchone()[0]
        print(f"  Transactions with invalid category_fk: {orphan_cats} [{'OK' if orphan_cats == 0 else 'ISSUE'}]")
        if orphan_cats > 0:
            issues.append(f"{orphan_cats} transactions reference non-existent categories")

        # Check all transaction wallet_fk references exist
        orphan_wallets = conn.execute("""
            SELECT COUNT(*) FROM transactions t
            WHERE t.wallet_fk NOT IN (SELECT wallet_pk FROM wallets)
              AND t.wallet_fk != '0'
        """).fetchone()[0]
        print(f"  Transactions with invalid wallet_fk: {orphan_wallets} [{'OK' if orphan_wallets == 0 else 'ISSUE'}]")
        if orphan_wallets > 0:
            issues.append(f"{orphan_wallets} transactions reference non-existent wallets")

        # Check subcategory references
        orphan_subcats = conn.execute("""
            SELECT COUNT(*) FROM categories c
            WHERE c.main_category_pk IS NOT NULL
              AND c.main_category_pk NOT IN (SELECT category_pk FROM categories)
        """).fetchone()[0]
        print(f"  Subcategories with invalid parent: {orphan_subcats} [{'OK' if orphan_subcats == 0 else 'ISSUE'}]")
        if orphan_subcats > 0:
            issues.append(f"{orphan_subcats} subcategories reference non-existent parents")

        # ── 4. Per-wallet transaction summary ──
        print("\n--- Per-Wallet Summary ---")
        wallet_stats = conn.execute("""
            SELECT w.name, w.currency,
                   COUNT(t.transaction_pk) as txn_count,
                   SUM(CASE WHEN t.income = 0 THEN t.amount ELSE 0 END) as total_expense,
                   SUM(CASE WHEN t.income = 1 THEN t.amount ELSE 0 END) as total_income
            FROM wallets w
            LEFT JOIN transactions t ON t.wallet_fk = w.wallet_pk
            GROUP BY w.wallet_pk
            ORDER BY txn_count DESC
        """).fetchall()
        for name, currency, count, expense, income in wallet_stats:
            curr = currency or "PKR"
            print(f"  {name} ({curr}): {count} txns, expenses={expense:,.0f}, income={income:,.0f}")

        # ── 5. Budget summary ──
        print("\n--- Budget Summary ---")
        budget_stats = conn.execute("""
            SELECT name, amount, added_transactions_only FROM budgets ORDER BY "order"
        """).fetchall()
        for name, amount, added_only in budget_stats:
            tag = " [label-derived]" if added_only else ""
            print(f"  {name}: {amount:,.0f}{tag}")

    finally:
        conn.close()

    # ── Final verdict ──
    print("\n" + "=" * 60)
    if issues:
        print(f"VALIDATION: {len(issues)} issue(s) found:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print("VALIDATION: All checks passed!")
        return True


if __name__ == "__main__":
    from src.extract import load_from_raw
    from src.generate import generate
    from src.transform import transform

    bb_data = load_from_raw()
    cashew_data = transform(bb_data)
    db_path = generate(cashew_data)
    validate(bb_data, cashew_data, db_path)
