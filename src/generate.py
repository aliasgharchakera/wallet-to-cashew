"""Generate a Cashew-compatible SQLite database from transformed data.

Matches Cashew's Drift ORM schema version 46.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.models import CashewData

OUTPUT_DIR = Path("data/output")
SCHEMA_VERSION = 46


def _dt_to_unix_ms(dt: datetime) -> int:
    """Convert datetime to Unix milliseconds (Drift stores dates this way)."""
    return int(dt.timestamp() * 1000)


def _dt_to_unix_ms_or_null(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    return _dt_to_unix_ms(dt)


def _create_tables(conn: sqlite3.Connection) -> None:
    """Create all Cashew database tables matching Drift schema v46."""
    conn.executescript("""
        -- Wallets / Accounts
        CREATE TABLE IF NOT EXISTS wallets (
            wallet_pk TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            colour TEXT,
            icon_name TEXT,
            date_created INTEGER NOT NULL,
            date_time_modified INTEGER NOT NULL,
            "order" INTEGER NOT NULL DEFAULT 0,
            currency TEXT,
            currency_format TEXT,
            decimals INTEGER NOT NULL DEFAULT 2,
            home_page_widget_display TEXT
        );

        -- Categories
        CREATE TABLE IF NOT EXISTS categories (
            category_pk TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            colour TEXT,
            icon_name TEXT,
            emoji_icon_name TEXT,
            date_created INTEGER NOT NULL,
            date_time_modified INTEGER NOT NULL,
            "order" INTEGER NOT NULL DEFAULT 0,
            income INTEGER NOT NULL DEFAULT 0,
            method_added INTEGER,
            main_category_pk TEXT
        );

        -- Transactions
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_pk TEXT NOT NULL PRIMARY KEY,
            paired_transaction_fk TEXT,
            name TEXT NOT NULL DEFAULT '',
            amount REAL NOT NULL DEFAULT 0.0,
            note TEXT NOT NULL DEFAULT '',
            category_fk TEXT NOT NULL,
            sub_category_fk TEXT,
            wallet_fk TEXT NOT NULL,
            date_created INTEGER NOT NULL,
            date_time_modified INTEGER NOT NULL,
            original_date_due INTEGER,
            income INTEGER NOT NULL DEFAULT 0,
            period_length INTEGER,
            reoccurrence INTEGER,
            end_date INTEGER,
            upcoming_transaction_notification INTEGER NOT NULL DEFAULT 1,
            type INTEGER,
            paid INTEGER NOT NULL DEFAULT 1,
            created_another_future_transaction INTEGER NOT NULL DEFAULT 0,
            skip_paid INTEGER NOT NULL DEFAULT 0,
            method_added INTEGER,
            transaction_owner_email TEXT,
            transaction_original_owner_email TEXT,
            shared_key TEXT,
            shared_old_key TEXT,
            shared_status INTEGER,
            shared_date_updated INTEGER,
            shared_reference_budget_pk TEXT,
            objective_fk TEXT,
            objective_loan_fk TEXT,
            budget_fks_exclude TEXT
        );

        -- Budgets
        CREATE TABLE IF NOT EXISTS budgets (
            budget_pk TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0.0,
            colour TEXT,
            start_date INTEGER NOT NULL,
            end_date INTEGER NOT NULL,
            wallet_fks TEXT,
            category_fks TEXT,
            category_fks_exclude TEXT,
            income INTEGER NOT NULL DEFAULT 0,
            archived INTEGER NOT NULL DEFAULT 0,
            added_transactions_only INTEGER NOT NULL DEFAULT 0,
            period_length INTEGER NOT NULL DEFAULT 1,
            reoccurrence INTEGER,
            date_created INTEGER NOT NULL,
            date_time_modified INTEGER NOT NULL,
            pinned INTEGER NOT NULL DEFAULT 0,
            "order" INTEGER NOT NULL DEFAULT 0,
            wallet_fk TEXT NOT NULL DEFAULT '0',
            budget_transaction_filters TEXT,
            member_transaction_filters TEXT,
            shared_key TEXT,
            shared_owner_member INTEGER,
            shared_date_updated INTEGER,
            shared_members TEXT,
            shared_all_members_ever TEXT,
            is_absolute_spending_limit INTEGER NOT NULL DEFAULT 0
        );

        -- Objectives (Goals & Loans)
        CREATE TABLE IF NOT EXISTS objectives (
            objective_pk TEXT NOT NULL PRIMARY KEY,
            type INTEGER NOT NULL DEFAULT 0,
            name TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0.0,
            "order" INTEGER NOT NULL DEFAULT 0,
            colour TEXT,
            date_created INTEGER NOT NULL,
            end_date INTEGER,
            date_time_modified INTEGER NOT NULL,
            icon_name TEXT,
            emoji_icon_name TEXT,
            income INTEGER NOT NULL DEFAULT 0,
            pinned INTEGER NOT NULL DEFAULT 1,
            archived INTEGER NOT NULL DEFAULT 0,
            wallet_fk TEXT NOT NULL DEFAULT '0'
        );

        -- Category Budget Limits
        CREATE TABLE IF NOT EXISTS category_budget_limits (
            category_limit_pk TEXT NOT NULL PRIMARY KEY,
            category_fk TEXT NOT NULL,
            budget_fk TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0.0,
            date_time_modified INTEGER NOT NULL,
            wallet_fk TEXT
        );

        -- Associated Titles (auto-categorization rules)
        CREATE TABLE IF NOT EXISTS associated_titles (
            associated_title_pk TEXT NOT NULL PRIMARY KEY,
            category_fk TEXT NOT NULL,
            title TEXT NOT NULL,
            date_created INTEGER NOT NULL,
            date_time_modified INTEGER NOT NULL,
            "order" INTEGER NOT NULL DEFAULT 0,
            is_exact_match INTEGER NOT NULL DEFAULT 0
        );

        -- Delete Logs
        CREATE TABLE IF NOT EXISTS delete_logs (
            delete_log_pk TEXT NOT NULL PRIMARY KEY,
            entry_pk TEXT NOT NULL,
            type INTEGER NOT NULL,
            date_time_modified INTEGER NOT NULL
        );

        -- Scanner Templates
        CREATE TABLE IF NOT EXISTS scanner_templates (
            scanner_template_pk TEXT NOT NULL PRIMARY KEY,
            date_created INTEGER NOT NULL,
            date_time_modified INTEGER NOT NULL,
            template_name TEXT NOT NULL,
            contains TEXT NOT NULL,
            title_transaction_before TEXT NOT NULL,
            title_transaction_after TEXT NOT NULL,
            amount_transaction_before TEXT NOT NULL,
            amount_transaction_after TEXT NOT NULL,
            default_category_fk TEXT NOT NULL,
            wallet_fk TEXT NOT NULL DEFAULT '0',
            ignore_field INTEGER NOT NULL DEFAULT 0
        );

        -- App Settings
        CREATE TABLE IF NOT EXISTS app_settings (
            settings_pk INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            settings_json TEXT NOT NULL DEFAULT '{}',
            date_updated INTEGER NOT NULL
        );
    """)


def _insert_wallets(conn: sqlite3.Connection, data: CashewData) -> None:
    conn.executemany(
        """INSERT INTO wallets (
            wallet_pk, name, colour, icon_name, date_created,
            date_time_modified, "order", currency, currency_format, decimals,
            home_page_widget_display
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                w.walletPk, w.name, w.colour, w.iconName,
                _dt_to_unix_ms(w.dateCreated), _dt_to_unix_ms(w.dateTimeModified),
                w.order, w.currency, w.currencyFormat, w.decimals,
                w.homePageWidgetDisplay,
            )
            for w in data.wallets
        ],
    )


def _insert_categories(conn: sqlite3.Connection, data: CashewData) -> None:
    conn.executemany(
        """INSERT INTO categories (
            category_pk, name, colour, icon_name, emoji_icon_name,
            date_created, date_time_modified, "order", income, method_added,
            main_category_pk
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                c.categoryPk, c.name, c.colour, c.iconName, c.emojiIconName,
                _dt_to_unix_ms(c.dateCreated), _dt_to_unix_ms(c.dateTimeModified),
                c.order, int(c.income), c.methodAdded, c.mainCategoryPk,
            )
            for c in data.categories
        ],
    )


def _insert_transactions(conn: sqlite3.Connection, data: CashewData) -> None:
    conn.executemany(
        """INSERT INTO transactions (
            transaction_pk, paired_transaction_fk, name, amount, note,
            category_fk, sub_category_fk, wallet_fk,
            date_created, date_time_modified, original_date_due,
            income, period_length, reoccurrence, end_date,
            upcoming_transaction_notification, type, paid,
            created_another_future_transaction, skip_paid, method_added,
            transaction_owner_email, transaction_original_owner_email,
            shared_key, shared_old_key, shared_status, shared_date_updated,
            shared_reference_budget_pk, objective_fk, objective_loan_fk,
            budget_fks_exclude
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                t.transactionPk, t.pairedTransactionFk, t.name, t.amount, t.note,
                t.categoryFk, t.subCategoryFk, t.walletFk,
                _dt_to_unix_ms(t.dateCreated), _dt_to_unix_ms(t.dateTimeModified),
                _dt_to_unix_ms_or_null(t.originalDateDue),
                int(t.income), t.periodLength, t.reoccurrence,
                _dt_to_unix_ms_or_null(t.endDate),
                int(t.upcomingTransactionNotification), t.type, int(t.paid),
                int(t.createdAnotherFutureTransaction), int(t.skipPaid),
                t.methodAdded, t.transactionOwnerEmail,
                t.transactionOriginalOwnerEmail,
                t.sharedKey, t.sharedOldKey, t.sharedStatus,
                _dt_to_unix_ms_or_null(t.sharedDateUpdated),
                t.sharedReferenceBudgetPk, t.objectiveFk, t.objectiveLoanFk,
                t.budgetFksExclude,
            )
            for t in data.transactions
        ],
    )


def _insert_budgets(conn: sqlite3.Connection, data: CashewData) -> None:
    conn.executemany(
        """INSERT INTO budgets (
            budget_pk, name, amount, colour, start_date, end_date,
            wallet_fks, category_fks, category_fks_exclude,
            income, archived, added_transactions_only,
            period_length, reoccurrence, date_created, date_time_modified,
            pinned, "order", wallet_fk, budget_transaction_filters,
            member_transaction_filters, shared_key, shared_owner_member,
            shared_date_updated, shared_members, shared_all_members_ever,
            is_absolute_spending_limit
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                b.budgetPk, b.name, b.amount, b.colour,
                _dt_to_unix_ms(b.startDate), _dt_to_unix_ms(b.endDate),
                b.walletFks, b.categoryFks, b.categoryFksExclude,
                int(b.income), int(b.archived), int(b.addedTransactionsOnly),
                b.periodLength, b.reoccurrence,
                _dt_to_unix_ms(b.dateCreated), _dt_to_unix_ms(b.dateTimeModified),
                int(b.pinned), b.order, b.walletFk, b.budgetTransactionFilters,
                b.memberTransactionFilters, b.sharedKey, b.sharedOwnerMember,
                _dt_to_unix_ms_or_null(b.sharedDateUpdated),
                b.sharedMembers, b.sharedAllMembersEver,
                int(b.isAbsoluteSpendingLimit),
            )
            for b in data.budgets
        ],
    )


def _insert_objectives(conn: sqlite3.Connection, data: CashewData) -> None:
    conn.executemany(
        """INSERT INTO objectives (
            objective_pk, type, name, amount, "order", colour,
            date_created, end_date, date_time_modified,
            icon_name, emoji_icon_name, income, pinned, archived, wallet_fk
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                o.objectivePk, o.type, o.name, o.amount, o.order, o.colour,
                _dt_to_unix_ms(o.dateCreated),
                _dt_to_unix_ms_or_null(o.endDate),
                _dt_to_unix_ms(o.dateTimeModified),
                o.iconName, o.emojiIconName, int(o.income),
                int(o.pinned), int(o.archived), o.walletFk,
            )
            for o in data.objectives
        ],
    )


def _insert_defaults(conn: sqlite3.Connection) -> None:
    """Insert default app settings and schema version marker."""
    now_ms = _dt_to_unix_ms(datetime.now(timezone.utc))
    conn.execute(
        "INSERT INTO app_settings (settings_json, date_updated) VALUES (?, ?)",
        ("{}", now_ms),
    )


def generate(data: CashewData, filename: str = "cashew-migrated.db") -> Path:
    """Generate a Cashew-compatible SQLite database file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    db_path = OUTPUT_DIR / filename

    # Remove existing file if present
    if db_path.exists():
        db_path.unlink()

    print(f"\nGenerating Cashew database at {db_path}...")

    conn = sqlite3.connect(str(db_path))
    try:
        # Set schema version (Drift uses PRAGMA user_version)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

        _create_tables(conn)
        print("  Tables created")

        _insert_wallets(conn, data)
        print(f"  Inserted {len(data.wallets)} wallets")

        _insert_categories(conn, data)
        print(f"  Inserted {len(data.categories)} categories")

        _insert_objectives(conn, data)
        print(f"  Inserted {len(data.objectives)} objectives")

        _insert_budgets(conn, data)
        print(f"  Inserted {len(data.budgets)} budgets")

        _insert_transactions(conn, data)
        print(f"  Inserted {len(data.transactions)} transactions")

        _insert_defaults(conn)
        print("  Inserted default settings")

        conn.commit()
        print(f"\nDatabase generated successfully: {db_path}")
        print(f"  Size: {db_path.stat().st_size / 1024:.1f} KB")

    finally:
        conn.close()

    return db_path


if __name__ == "__main__":
    from src.extract import load_from_raw
    from src.transform import transform

    bb_data = load_from_raw()
    cashew_data = transform(bb_data)
    generate(cashew_data)
